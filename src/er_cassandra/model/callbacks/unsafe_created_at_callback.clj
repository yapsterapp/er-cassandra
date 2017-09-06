(ns er-cassandra.model.callbacks.unsafe-created-at-callback
  (:require
   [clj-time.core :as t]
   [clj-uuid :as uuid]))

(defn unsafe-created-at-callback
  "it's sort of kinda like a created-at, but it's not very safe,
   because there is no sane way of providing the guarantee"
  ([] (unsafe-created-at-callback :created_at))
  ([col]
   (fn [r]
     (let [v (get r col)]
       (if v
         r
         (assoc r col (.toDate (t/now))))))))

(defn unsafe-created-at-timeuuid-callback
  ([] (unsafe-created-at-timeuuid-callback :created_at_timeuuid))
  ([col]
   (fn [r]
     (let [v (get r col)]
       (if v
         r
         (assoc r col (uuid/v1)))))))

(defn timestamp-from-timeuuid-callback
  [timeuuid-col timestamp-col]
  (fn [r]
    (let [t-uuid (get r timeuuid-col)]
      (if (uuid/uuid? t-uuid)
        (assoc r timestamp-col (uuid/get-instant t-uuid))
        r))))
