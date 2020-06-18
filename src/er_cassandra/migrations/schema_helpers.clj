(ns er-cassandra.migrations.schema-helpers
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [schema.core :as schema]
   [er-cassandra.schema :as c*.schema]
   [er-cassandra.session :as session]
   [er-cassandra.model.types :as t]
   [prpr.promise :as prpr])
  (:import
   [er_cassandra.session Session]))

(defn index-str
  [col-groups]
  (-> (reduce
       (fn [rs cg]
         (if (sequential? cg)
           (str rs "," (index-str cg))
           (str rs "," (name cg))))
       ""
       col-groups)
      (string/replace-first \,\()
      (str \))))

(def clustering-order-directions
  #{:asc :desc})

(defn clustering-order-entry?
  [x]
  (if (sequential? x)
    (and (= 2 (count x)) (some? (clustering-order-directions (last x))))
    true))

(defn clustering-order-entry-str
  [e]
  (if (sequential? e)
    (let [[col-name dir] e]
      (str (name col-name) " " (string/upper-case (name dir))))
    (str (name e) " ASC")))

(defn clustering-order-str
  [clustering-order]
  (-> (reduce
       (fn [rs cg]
         (if (clustering-order-entry? cg)
           (str rs ", " (clustering-order-entry-str cg))
           (str rs ", " (clustering-order-str cg))))
       ""
       clustering-order)
      (string/replace-first #", " "")))

(defn drop-view
  [{v-name :name}]
  (string/join " " ["drop materialized view if exists" (name v-name)]))

(defn drop-table
  [{v-name :name}]
  (string/join " " ["drop table if exists" (name v-name)]))

(def leveled-compaction-clause
  "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'tombstone_compaction_interval': '86400'}")

(def size-tiered-compaction-clause
  "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}")

(schema/defschema CassandraTableCompaction
  (schema/enum nil :leveled :size-tiered))

(schema/defschema CassandraColumnType
  schema/Keyword)

(schema/defschema CassandraClusteringOrderDirection
  (apply schema/enum clustering-order-directions))

(schema/defschema CassandraColumnClusteringOrder
  (schema/conditional
   (fn [co] (sequential? co))
   [(schema/one schema/Keyword :name)
    (schema/optional CassandraClusteringOrderDirection :dir)]

   :else
   schema/Keyword))

(schema/defschema CassandraClusteringOrder
  [CassandraColumnClusteringOrder])

(schema/defschema CreateTable
  {:name schema/Str
   :primary-key t/PrimaryKeySchema
   :columns [[(schema/one schema/Keyword :name)
              (schema/one CassandraColumnType :type)]]
   (schema/optional-key :compaction) CassandraTableCompaction
   (schema/optional-key :clustering-order) CassandraClusteringOrder
   (schema/optional-key :default-ttl) schema/Num})

(defn all-primary-key-columns-exist?
  [columns primary-key]
  (let [column-names (set columns)]
    (every?
     #(contains? column-names %)
     (flatten primary-key))))

(defn all-columns-are-keywords?
  [columns]
  (every? keyword? columns))

(defn primary-key-contains-clustering-order-cols?
  [primary-key clustering-order]
  (let [primary-key-cols (set (flatten primary-key))
        clustering-order-cols (->> clustering-order
                                   flatten
                                   (remove #(clustering-order-directions %)))]
    (every?
     #(contains? primary-key-cols %)
     clustering-order-cols)))

(schema/defn ^:always-validate create-table
  [{v-name :name
    v-primary-key :primary-key
    v-columns :columns
    v-compaction :compaction
    v-clustering-order :clustering-order
    v-default-ttl :default-ttl
    :as table-definition} :- CreateTable]
  (assert (every? some? [v-name v-primary-key v-columns])
          (str "invalid table definition"
               "\n\n"
               (pr-str table-definition)))
  (assert (all-primary-key-columns-exist? (map first v-columns) v-primary-key)
          (str "not all primary key columns are defined!"
               "\n\n"
               (pr-str table-definition)))
  (assert (all-columns-are-keywords? (map first v-columns))
          (str "column names must be keywords!"
               "\n\n"
               (pr-str table-definition)))
  (assert (primary-key-contains-clustering-order-cols? v-primary-key v-clustering-order)
          (str "not all cluster ordering columns are in the primary key!"
               "\n\n"
               (pr-str table-definition)))
  (assert (or (nil? v-compaction) (#{:leveled :size-tiered} v-compaction)))
  (let [v-name (name v-name)
        v-columns (map (fn [[n t]] (str (name n) " " (name t))) v-columns)
        primary-key-str (str "primary key " (index-str v-primary-key))
        clustering-order-str (when (some? v-clustering-order)
                               (str "CLUSTERING ORDER BY ("
                                    (clustering-order-str v-clustering-order)
                                    ")"))
        compaction-str (case v-compaction
                         nil leveled-compaction-clause
                         :leveled leveled-compaction-clause
                         :size-tiered size-tiered-compaction-clause)
        default-ttl-str (when (some? v-default-ttl)
                          (str "default_time_to_live = " v-default-ttl))
        with-strs (filter
                   identity
                   [clustering-order-str
                    compaction-str
                    default-ttl-str])]
    (string/join
     " "
     ["create table if not exists" v-name "("
      (string/join
       ", "
       (conj
        v-columns
        primary-key-str))
      ")"
      (when (seq with-strs)
        (str "WITH " (string/join " AND " with-strs)))])))

(schema/defschema CreateView
  {:name schema/Str
   :from schema/Str
   :primary-key t/PrimaryKeySchema
   :selected-columns [schema/Keyword]
   (schema/optional-key :compaction) CassandraTableCompaction
   (schema/optional-key :clustering-order) CassandraClusteringOrder})

(schema/defn ^:always-validate create-view
  [{v-name :name
    v-from :from
    v-primary-key :primary-key
    v-selected-columns :selected-columns
    v-compaction :compaction
    v-clustering-order :clustering-order
    :as view-definition} :- CreateView]
  (assert (every? some? [v-name v-from v-primary-key v-selected-columns])
          (str "invalid view definition"
               "\n\n"
               (pr-str view-definition)))
  (assert (all-primary-key-columns-exist? v-selected-columns v-primary-key)
          (str "views must include all primary key columns"
               "\n\n"
               (pr-str view-definition)))
  (assert (all-columns-are-keywords? v-selected-columns)
          (str "column names must be keywords!"
               "\n\n"
               (pr-str view-definition)))
  (assert (primary-key-contains-clustering-order-cols? v-primary-key v-clustering-order)
          (str "not all cluster ordering columns are in the primary key!"
               "\n\n"
               (pr-str view-definition)))
  (assert (or (nil? v-compaction) (#{:leveled :size-tiered} v-compaction)))
  (let [v-name (name v-name)
        v-from (name v-from)
        v-selected-columns (map name v-selected-columns)
        primary-key-columns (->> v-primary-key flatten (map name))
        where-clauses (string/join
                       " and "
                       (map #(str % " is not null") primary-key-columns))
        primary-key-str (index-str v-primary-key)
        clustering-order-str (when (some? v-clustering-order)
                               (str "CLUSTERING ORDER BY ("
                                    (clustering-order-str v-clustering-order)
                                    ")"))
        compaction-str (case v-compaction
                         nil leveled-compaction-clause
                         :leveled leveled-compaction-clause
                         :size-tiered size-tiered-compaction-clause)
        with-strs (filter
                   identity
                   [clustering-order-str
                    compaction-str])]
    (string/join
     " "
     ["create materialized view if not exists" v-name
      "as select"
      (string/join ", " v-selected-columns)
      "from" v-from
      "where" where-clauses
      "primary key" primary-key-str
      (when (seq with-strs)
        (str "WITH " (string/join " AND " with-strs)))])))

;;
;; derivation from C* metadata
;;

(defn raw-compaction->compaction-definition
  [{compaction-class "class" :as compaction}]
  (case compaction-class
    "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"
    :leveled

    "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"
    :size-tiered

    #_else
    nil))

(defn derive-primary-key
  [partitioning-cols clustering-cols]
  (let [partition-key (reduce
                       (fn [rs [_ n]]
                         (conj rs n))
                       []
                       (sort-by first partitioning-cols))
        clustering-key (reduce
                        (fn [rs [_ n]]
                          (conj rs n))
                        []
                        (sort-by first clustering-cols))]
    (into [partition-key] clustering-key)))

(defn derive-clustering-order
  [clustering-cols]
  (reduce
   (fn [rs [_ n d]]
     (conj rs [n d]))
   []
   (sort-by first clustering-cols)))

(defn simple-col-type?
  [t]
  (not (re-find #"<" t)))

(defn columns-metadata->columns-definitions
  [metadata]
  (loop [columns []
         partitioning-cols []
         clustering-cols []
         [col-metadata & rest-metadata] metadata]
    (let [{col-name-str :column_name
           col-type-str :type
           kind :kind
           position :position
           cluster-order-str :clustering_order} col-metadata
          col-name (keyword col-name-str)
          col-type (if (simple-col-type? col-type-str)
                     (keyword col-type-str)
                     col-type-str)
          cluster-order (keyword cluster-order-str)
          add-to-partition? (= "partition_key" kind)
          add-to-cluster? (= "clustering" kind)
          columns (conj columns [col-name col-type])
          partitioning-cols (if add-to-partition?
                              (conj partitioning-cols [position col-name])
                              partitioning-cols)
          clustering-cols (if add-to-cluster?
                            (conj clustering-cols [position col-name cluster-order])
                            clustering-cols)]
      (if (empty? rest-metadata)
        (reduce-kv
         (fn [rs k v]
           (if (seq v)
             (assoc rs k v)
             rs))
         {}
         {:columns columns
          :primary-key (derive-primary-key partitioning-cols clustering-cols)
          :clustering-order (derive-clustering-order clustering-cols)})
        (recur columns partitioning-cols clustering-cols rest-metadata)))))

(defn table-definition
  ([^Session session table-name]
   (table-definition session (session/keyspace session) table-name))
  ([^Session session keyspace table-name]
   (prpr/ddo
    [{compaction :compaction
      default-ttl :default_time_to_live
      :as metadata} (c*.schema/table-metadata session keyspace table-name)
     columns (c*.schema/table-columns-metadata session keyspace table-name)
     :let [col-defs (columns-metadata->columns-definitions columns)
           cmpct-def (raw-compaction->compaction-definition compaction)]]
    (prpr/return
     (merge
      {:name table-name}
      (when (some? cmpct-def)
        {:compaction cmpct-def})
      (when (some? default-ttl)
        {:default-ttl default-ttl})
      col-defs)))))

(defn view-definition
  ([^Session session view-name]
   (view-definition session (session/keyspace session) view-name))
  ([^Session session keyspace view-name]
   (prpr/ddo
    [{table-name :base_table_name
      compaction :compaction
      :as metadata} (c*.schema/view-metadata session keyspace view-name)
     columns (c*.schema/view-columns-metadata session keyspace view-name)
     :let [col-defs (columns-metadata->columns-definitions columns)
           sel-cols (mapv first (:columns col-defs))
           cmpct-def (raw-compaction->compaction-definition compaction)]]
    (prpr/return
     (merge
      {:name view-name
       :from table-name
       :selected-columns sel-cols}
      (when (some? cmpct-def)
        {:compaction cmpct-def})
      (dissoc col-defs :columns))))))

;;
;; some helpers for view updates
;;

(defn missing-selected-columns?
  [{selected-columns :selected-columns}
   columns]
  (let [missing-cols (set/difference (set columns) (set selected-columns))]
    (some-> (seq missing-cols) set)))

(defn add-selected-columns-to-view
  [{view-columns :selected-columns
    :as view-def}
   columns]
  (let [cols-to-add (set/difference (set columns) (set view-columns))]
    (if (seq cols-to-add)
      (update view-def :selected-columns into cols-to-add)
      view-def)))

(defn drop-selected-columns-from-view
  [{view-columns :selected-columns
    :as view-def}
   columns]
  (let [cols-to-drop (set/intersection (set columns) (set view-columns))]
    (if (seq cols-to-drop)
      (update view-def :selected-columns (comp vec (partial remove cols-to-drop)))
      view-def)))
