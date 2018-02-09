(ns er-cassandra.migrations.schema-helpers
  (:require
   [clojure.string :as string]))

(defn index-str
  [col-groups]
  (-> (reduce
       (fn [rs cg]
         (if (sequential? cg)
           (str rs "," (index-str cg))
           (str rs "," cg)))
       ""
       col-groups)
      (string/replace-first \,\()
      (str \))))

(defn drop-view
  [{name :name}]
  (string/join " " ["drop materialized view if exists" name]))

(def leveled-compaction-clause
  "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'tombstone_compaction_interval': '86400'}")

(def size-tiered-compaction-clause
  "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}")

(defn create-view
  [{:keys [name from primary-key selected-columns compaction]
    :as view-definition}]
  (assert (every? some? [name from primary-key selected-columns])
          (str "invalid view definition"
               "\n\n"
               (pr-str view-definition)))
  (assert (every? #(contains? (set selected-columns) %) (flatten primary-key))
          (str "views must include all primary key columns"
               "\n\n"
               (pr-str view-definition)))
  (assert (or (nil? compaction) (#{:leveled :size-tiered} compaction)))
  (let [primary-key-columns (flatten primary-key)
        where-clauses (string/join
                       " and "
                       (map #(str % " is not null") primary-key-columns))
        primary-key-str (index-str primary-key)]
    (string/join
     " "
     ["create materialized view if not exists" name
      "as select"
      (string/join ", " selected-columns)
      "from" from
      "where" where-clauses
      "primary key" primary-key-str
      (case compaction
        nil nil
        :leveled (str "WITH " leveled-compaction-clause)
        :size-tiered (str "WITH " size-tiered-compaction-clause))])))
