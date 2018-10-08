(ns er-cassandra.util.vector)

(defn coerce
  [obj]
  (cond
    (nil? obj) nil
    (vector? obj) obj
    (sequential? obj) (vec obj)
    :else [obj]))
