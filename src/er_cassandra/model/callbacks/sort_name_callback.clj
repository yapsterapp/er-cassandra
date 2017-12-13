(ns er-cassandra.model.callbacks.sort-name-callback
  (:require
   [er-cassandra.util.string :refer [normalize-string]]))

(defn create-sort-name-callback
  [sort-col name-col]
  (fn [r]
    (if (contains? r name-col)
      (assoc r sort-col (normalize-string (get r name-col)))
      r)))
