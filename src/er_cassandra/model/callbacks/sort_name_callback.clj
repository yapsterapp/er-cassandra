(ns er-cassandra.model.callbacks.sort-name-callback
  (:require
   [er-cassandra.util.string :refer [normalize-string]]))

(defn create-sort-name-callback
  [sort-col name-col]
  (fn [r]
    (assoc r sort-col (normalize-string (get r name-col)))))
