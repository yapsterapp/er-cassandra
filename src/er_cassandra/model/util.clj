(ns er-cassandra.model.util
  (:require
   [manifold.deferred :as d]
   [cats.monad.either :as either]))

(defn combine-responses
  "combine a seq of Deferred[Either] responses into
   a single Deferred[Either] response"
  [responses]
  (d/chain
   (apply d/zip responses)
   (fn [responses]
     (if (some either/left? responses)
       (either/left responses)

       (either/right (mapv deref responses))))))

(defn create-lookup-record
  "construct a lookup record"
  [uber-key uber-key-value key key-value]
  (into
   {}
   (concat (map vector uber-key uber-key-value)
           (map vector key key-value))))
