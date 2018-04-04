(ns er-cassandra.migrations.lookup-table-helpers
  (:require
   [clojure.set :as set]
   [er-cassandra.record :as r]
   [manifold.stream :as stream]
   [prpr.promise :refer [ddo]]))

(defn populate-lookup-table
  [cassandra
   {source-name :name
    source-columns :columns
    :as source-table}
   {lookup-name :name
    lookup-columns :columns
    lookup-key :primary-key
    :as lookup-table}]
  (ddo [:let [shared-columns (map
                              keyword
                              (set/intersection
                               (set (map first source-columns))
                               (set (map first lookup-columns))))
              lookup-key-columns (map keyword (flatten lookup-key))]
        source-records (r/select-buffered
                        cassandra
                        (keyword source-name))
        :let [lookup-records (->> source-records
                                  (stream/filter
                                   (fn [r]
                                     (every?
                                      (fn [c] (some? (get r c)))
                                      lookup-key-columns)))
                                  (stream/map
                                   (fn [r]
                                     (select-keys r shared-columns))))]
        insert-stream (r/insert-buffered
                       cassandra
                       (keyword lookup-name)
                       lookup-records)]
       (stream/reduce (constantly true) insert-stream)))
