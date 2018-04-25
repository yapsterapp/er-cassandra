(ns er-cassandra.model.alia.minimal-change
  (:require
   [clojure.set :as set]
   [prpr.promise :as pr]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.lookup :as lookup]
   [taoensso.timbre :refer [info warn]])
  (:import
   [er_cassandra.model.types Entity]))

(defn change-cols
  "returns non-key columns which have changes between
   old-record and record"
  [key-col-set
   old-record
   record]
  (let [record-col-set (->> record keys set)
        other-col-set (set/difference
                       record-col-set
                       key-col-set)]

    (->> other-col-set
         (filter
          #(not= (get old-record %) (get record %))))))

(defn minimal-change-for-table
  "return a minimal change record for a table
   - if there are changes it contains the key cols and changed cols
   - if there are no changes and there was an old record it is nil
   - if there are no changes and there was no old record it contains
     just the key cols"
  [{t-k :key
    :as table}
   old-record
   record]
  (let [key-col-set (->> t-k flatten set)
        ch-cols (change-cols key-col-set old-record record)]

    (if (and (not-empty old-record)
             (empty? ch-cols))
      nil
      (select-keys record (concat key-col-set ch-cols)))))
