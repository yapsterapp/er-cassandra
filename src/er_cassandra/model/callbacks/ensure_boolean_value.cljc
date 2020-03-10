(ns er-cassandra.model.callbacks.ensure-boolean-value)

(defn ensure-boolean-value-callback
  [col-key]
  (fn [record]
    (if (contains? record col-key)
      (update record col-key boolean)
      record)))
