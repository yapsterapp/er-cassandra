(ns er-cassandra.model.delete
  (:require
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [manifold.deferred :as d]
   [prpr.promise :as promise :refer [ddo]]
   [prpr.stream :as s]
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.util :refer [combine-responses]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn delete
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (delete session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]

   (ms/-delete session entity key record-or-key-value opts)))

(defn delete-buffered
  "delete each record in a Stream<record>, optionally controlling
   concurrency with :buffer-size. returns a Deferred<Stream<response>>
   of delete responses"
  ([^ModelSession session ^Entity entity key record-stream]
   (delete-buffered session entity key record-stream {:buffer-size 25}))
  ([^ModelSession session
    ^Entity entity
    key
    record-stream
    {:keys [buffer-size] :as opts}]
   (->> record-stream
        (s/map-concurrently
         (or buffer-size 25)
         #(delete session entity key % (dissoc opts :buffer-size)))
        (return deferred-context))))

(defn delete-many
  "issue one delete query for each record and combine the responses"
  [^ModelSession session ^Entity entity key records]
  (ddo [dr-s (delete-buffered session entity key records)]
    (->> dr-s
         (s/reduce conj []))))
