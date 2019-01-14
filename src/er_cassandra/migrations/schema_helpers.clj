(ns er-cassandra.migrations.schema-helpers
  (:require
   [clojure.string :as string]))

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

(defn all-primary-key-columns-exist?
  [columns primary-key]
  (let [column-names (set columns)]
    (every?
     #(contains? column-names %)
     (flatten primary-key))))

(defn primary-key-contains-clustering-order-cols?
  [primary-key clustering-order]
  (let [primary-key-cols (set (flatten primary-key))
        clustering-order-cols (->> clustering-order
                                   flatten
                                   (remove #(clustering-order-directions %)))]
    (every?
     #(contains? primary-key-cols %)
     clustering-order-cols)))

(defn create-table
  [{v-name :name
    v-primary-key :primary-key
    v-columns :columns
    v-compaction :compaction
    v-clustering-order :clustering-order
    :as table-definition}]
  (assert (every? some? [v-name v-primary-key v-columns])
          (str "invalid table definition"
               "\n\n"
               (pr-str table-definition)))
  (assert (all-primary-key-columns-exist? (map first v-columns) v-primary-key)
          (str "not all primary key columns are defined!"
               "\n\n"
               (pr-str table-definition)))
  (assert (primary-key-contains-clustering-order-cols? v-primary-key v-clustering-order)
          (str "not all cluster ordering columns are in the primary key!"
               "\n\n"
               (pr-str table-definition)))
  (assert (or (nil? v-compaction) (#{:leveled :size-tiered} v-compaction)))
  (let [v-name (name v-name)
        v-columns (map (fn [[n t]] (str n " " t)) v-columns)
        primary-key-str (str "primary key " (index-str v-primary-key))
        clustering-order-str (when (some? v-clustering-order)
                               (str "CLUSTERING ORDER BY ("
                                    (clustering-order-str v-clustering-order)
                                    ")"))
        compaction-str (case v-compaction
                         nil nil
                         :leveled leveled-compaction-clause
                         :size-tiered size-tiered-compaction-clause)
        with-strs (filter
                   identity
                   [clustering-order-str
                    compaction-str])]
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

(defn create-view
  [{v-name :name
    v-from :from
    v-primary-key :primary-key
    v-selected-columns :selected-columns
    v-compaction :compaction
    v-clustering-order :clustering-order
    :as view-definition}]
  (assert (every? some? [v-name v-from v-primary-key v-selected-columns])
          (str "invalid view definition"
               "\n\n"
               (pr-str view-definition)))
  (assert (all-primary-key-columns-exist? v-selected-columns v-primary-key)
          (str "views must include all primary key columns"
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
                         nil nil
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
