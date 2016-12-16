(ns er-cassandra.util.sequential)

(defn coerce
  ([obj] (coerce obj []))
  ([obj default]
   (cond
     (nil? obj) default
     (sequential? obj) obj
     :else [obj])))
