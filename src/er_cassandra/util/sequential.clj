(ns er-cassandra.util.sequential)

(defn coerce
  [obj]
  (cond
    (nil? obj) nil
    (sequential? obj) obj
    :else [obj]))
