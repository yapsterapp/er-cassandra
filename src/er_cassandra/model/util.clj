(ns er-cassandra.model.util
  (:require
   [manifold.deferred :as d]
   [cats.monad.either :as either]))

(defn combine-responses
  "combine a seq of Deferred responses into
   a single Deferred response"
  [responses]
  (d/chain (apply d/zip responses)
           (fn [r] (filter identity r))))

(defn combine-seq-responses
  "combine a seq of Deferred seq responses into
   a single Deferred response of a concatenated seq"
  [responses]
  (d/chain (apply d/zip responses)
           (fn [rs] (apply concat rs))))

(defn create-lookup-record
  "construct a lookup record"
  [uber-key uber-key-value key key-value]
  (into
   {}
   (concat (map vector (flatten uber-key) uber-key-value)
           (map vector (flatten key) key-value))))
