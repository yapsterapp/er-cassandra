(ns er-cassandra.model.callbacks.distinct-vector-callback)

(defn create-distinct-vector-callback
  [col]
  (fn [r]
    (assoc r (vec (distinct (get r col))))))
