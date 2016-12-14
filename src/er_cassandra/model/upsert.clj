(ns er-cassandra.model.upsert
  (:require
   [er-cassandra.model.util :refer [combine-responses]]
   [er-cassandra.model.model-session :as ms])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

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

(defn upsert-many
  "issue one upsert query for each record and combine the responses"
  [^ModelSession session ^Entity entity records]
  (->> records
       (map (fn [record]
              (upsert session entity record)))
       combine-responses))
