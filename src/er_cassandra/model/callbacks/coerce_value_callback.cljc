(ns er-cassandra.model.callbacks.coerce-value-callback)

(defn coerce-value-callback
  [col f]
  (fn [r]
    (if (contains? r col)
      (assoc r col (f (get r col)))
      r)))
