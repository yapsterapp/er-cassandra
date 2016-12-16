(ns er-cassandra.util.vector)

(defn coerce
  ([obj] (coerce obj []))
  ([obj default]
   (cond
     (nil? obj) default
     (vector? obj) obj
     (sequential? obj) (vec obj)
     :else [obj])))
