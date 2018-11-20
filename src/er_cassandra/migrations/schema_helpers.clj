(ns er-cassandra.migrations.schema-helpers
  (:require
   [clojure.string :as string]
   [schema.core :as schema]
   [er-cassandra.model.types :as t]))

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
  (schema/conditional
   keyword? schema/Keyword
   string? schema/Str))

(schema/defschema CreateTable
  {:name schema/Str
   :primary-key t/PrimaryKeySchema
   :columns [[(schema/one schema/Keyword :name)
              (schema/one CassandraColumnType :type)]]
   :compaction CassandraTableCompaction})

(schema/defn create-table
  [{v-name :name
    v-primary-key :primary-key
    v-columns :columns
    v-compaction :compaction
    :as table-definition} :- CreateTable]
  (assert (every? some? [v-name v-primary-key v-columns])
          (str "invalid table definition"
               "\n\n"
               (pr-str table-definition)))
  (assert (every? #(contains? (set (map first v-columns)) %) (flatten v-primary-key))
          (str "not all primary key columns are defined!"
               "\n\n"
               (pr-str table-definition)))
  (assert (or (nil? v-compaction) (#{:leveled :size-tiered} v-compaction)))
  (let [v-name (name v-name)
        v-columns (map (fn [[n t]] (str (name n) " " (name t))) v-columns)
        primary-key-str (str "primary key " (index-str v-primary-key))]
    (string/join
     " "
     ["create table if not exists" v-name "("
      (string/join
       ", "
       (conj
        v-columns
        primary-key-str))
      ")"
      (case v-compaction
        nil (str "WITH " leveled-compaction-clause)
        :leveled (str "WITH " leveled-compaction-clause)
        :size-tiered (str "WITH " size-tiered-compaction-clause))])))

(schema/defschema CreateView
  {:name schema/Str
   :from schema/Str
   :primary-key t/PrimaryKeySchema
   :selected-columns [schema/Keyword]
   :compaction CassandraTableCompaction})

(schema/defn create-view
  [{v-name :name
    v-from :from
    v-primary-key :primary-key
    v-selected-columns :selected-columns
    v-compaction :compaction
    :as view-definition} :- CreateView]
  (assert (every? some? [v-name v-from v-primary-key v-selected-columns])
          (str "invalid view definition"
               "\n\n"
               (pr-str view-definition)))
  (assert (every? #(contains? (set v-selected-columns) %) (flatten v-primary-key))
          (str "views must include all primary key columns"
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
        primary-key-str (index-str v-primary-key)]
    (string/join
     " "
     ["create materialized view if not exists" v-name
      "as select"
      (string/join ", " v-selected-columns)
      "from" v-from
      "where" where-clauses
      "primary key" primary-key-str
      (case v-compaction
        nil (str "WITH " leveled-compaction-clause)
        :leveled (str "WITH " leveled-compaction-clause)
        :size-tiered (str "WITH " size-tiered-compaction-clause))])))
