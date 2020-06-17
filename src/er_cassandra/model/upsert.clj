(ns er-cassandra.model.upsert
  (:require
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [prpr.promise :as promise :refer [ddo]]
   [prpr.stream :as s]
   [er-cassandra.model.model-session :as ms])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn change
  "change a single instance given the previous version of the instance

   returns a Deferred<[:upsert updated-record key-failures]> where
   updated-record is the record as currently in the db and key-failures
   in nil"
  ([^ModelSession session ^Entity entity old-record record]
   (change session entity old-record record {}))
  ([^ModelSession session ^Entity entity old-record record opts]
   (ms/-change session entity old-record record opts)))

(defn ^:deprecated upsert
  "**DEPRECATED** use `select-upsert` or `change` instead

   upsert a single instance

   throws an Exception if the entity has
   any foreign keys, since a re-select would be required to determine
   the foreign key ops required. use change or select-upsert instead
   for entities with foreign keys

   throws an Exception if the record contains non-serialized collection
   columns as these cannot be upserted safely without generating tombstones
   use select-upsert (or change) instead for entities with non-serialized
   collection columns

   returns a Deferred<[updated-record key-failures]> where
   updated-record is the record as currently in the db and key-failures
   in nil"
  ([^ModelSession session ^Entity entity record]
   (upsert session entity record {}))
  ([^ModelSession session ^Entity entity record opts]
   (ms/-upsert session entity record opts)))

(defn select-upsert
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records

   will re-select the current version of the record
   if necessary (if the entity has any foreign keys)

   returns a Deferred[Pair[updated-record key-failures]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired "
  ([^ModelSession session ^Entity entity record]
   (select-upsert session entity record {}))
  ([^ModelSession session ^Entity entity record opts]
   (ms/-select-upsert session entity record opts)))

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
   (->> record-stream
        (s/map-concurrently
         (or buffer-size 25)
         (fn [[o-r r]]
           (change session
                   entity
                   o-r
                   r
                   (dissoc opts :buffer-size))))
        (return deferred-context))))

(defn ^:deprecated upsert-buffered
  "upsert each record in a Stream<record>, optionally controlling
   concurrency with :buffer-size. returns a Deferred<Stream<response>>
   of upsert responses

   use change-buffered instead plz. this uses select-upsert internally "
  ([^ModelSession session ^Entity entity record-stream]
   (upsert-buffered session entity record-stream {:buffer-size 25}))
  ([^ModelSession session
    ^Entity entity
    record-stream
    {:keys [buffer-size] :as opts}]
   (->> record-stream
        (s/map-concurrently
         (or buffer-size 25)
         #(select-upsert session entity % (dissoc opts :buffer-size)))
        (return deferred-context))))

(defn ^:deprecated upsert-many
  "issue one upsert query for each record and combine the responses"
  [^ModelSession session ^Entity entity records]
  (ddo [ups-s (upsert-buffered session entity records)]
    (->> ups-s
         (s/reduce conj []))))
