(ns er-cassandra.model.upsert
  (:require
   [er-cassandra.model.util :refer [combine-responses]]
   [er-cassandra.model.model-session :as ms])
  (:import
   [er_cassandra.model.types Model]
   [er_cassandra.model.model_session ModelSession]))

(defn upsert
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records

   returns a Deferred[Pair[updated-record key-failures]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired "
  ([^ModelSession session ^Model model record]
   (upsert session model record {}))
  ([^ModelSession session ^Model model record opts]

   (ms/-upsert session model record opts)))

(defn upsert-many
  "issue one upsert query for each record and combine the responses"
  [^ModelSession session ^Model model records]
  (->> records
       (map (fn [record]
              (upsert session model record)))
       combine-responses))
