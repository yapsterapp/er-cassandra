(ns er-cassandra.model.callbacks.unsafe-created-at-callback
  (:require
   [clj-time.core :as t]))

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
