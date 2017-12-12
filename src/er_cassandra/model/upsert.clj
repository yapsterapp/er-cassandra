(ns er-cassandra.model.upsert
  (:require
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [er-cassandra.model.util :refer [combine-responses]]
   [er-cassandra.model.model-session :as ms])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn change
  ([^ModelSession session ^Entity entity old-record record]
   (change session entity old-record record {}))
  ([^ModelSession session ^Entity entity old-record record opts]
   (ms/-change session entity old-record record opts)))

(defn upsert
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records

   returns a Deferred[Pair[updated-record key-failures]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired "
  ([^ModelSession session ^Entity entity record]
   (upsert session entity record {}))
  ([^ModelSession session ^Entity entity record opts]
   (ms/-upsert session entity record opts)))

(defn change-buffered
  "upsert-changes frmo a Stream<[old-record record]>,
  optionally controlling concurrency with :buffer-size.
  returns a Deferred<Stream<response>> of upsert responses"
  ([^ModelSession session ^Entity entity record-stream]
   (change-buffered session
                    entity
                    record-stream
                    {:buffer-size 25}))
  ([^ModelSession session
    ^Entity entity
    record-stream
    {:keys [buffer-size] :as opts}]
   (with-context deferred-context
     (->> record-stream
          (s/map
           (fn [[o-r r]]
             (change session
                     entity
                     o-r
                     r
                     (dissoc opts :buffer-size))))
          ((fn [s]
             (if buffer-size
               (s/buffer buffer-size s)
               s)))
          return))))

(defn upsert-buffered
  "upsert each record in a Stream<record>, optionally controlling
   concurrency with :buffer-size. returns a Deferred<Stream<response>>
   of upsert responses"
  ([^ModelSession session ^Entity entity record-stream]
   (upsert-buffered session entity record-stream {:buffer-size 25}))
  ([^ModelSession session
    ^Entity entity
    record-stream
    {:keys [buffer-size] :as opts}]
   (with-context deferred-context
     (->> record-stream
          (s/map #(upsert session entity % (dissoc opts :buffer-size)))
          ((fn [s]
             (if buffer-size
               (s/buffer buffer-size s)
               s)))
          return))))

(defn ^:deprecated upsert-many
  "issue one upsert query for each record and combine the responses"
  [^ModelSession session ^Entity entity records]
  (with-context deferred-context
    (mlet [ups-s (upsert-buffered session entity records)]
      (->> ups-s
           (s/reduce conj [])))))
