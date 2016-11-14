(ns er-cassandra.model.callbacks.distinct-vector-callback
  (:require
   [plumbing.core :refer :all]))

(defn create-distinct-vector-callback
  [col]
  (fn [r]
    (let [v (get r col)]
      (assoc-when r col (when v (vec (distinct v)))))))
