(ns er-cassandra.model.upsert
  (:require
   [clojure.set :as set]
   [clojure.core.match :refer [match]]
   [manifold.deferred :as d]
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]]
   [er-cassandra.model.unique-key :as unique-key]
   [er-cassandra.model.util :as util])
  (:import [er_cassandra.model.types Model]))

(defn delete-record
  [session model table key-value]
  (with-context deferred-context
    (mlet [delete-result (r/delete session
                                   (:name table)
                                   (:key table)
                                   key-value)]
      (return
       [:ok {:table (:name table)
             :key (:key table)
             :key-value key-value} :deleted]))))

(defn upsert-record
  [session ^Model model table record]
  (with-context deferred-context
    (mlet [insert-result (r/insert session
                                   (:name table)
                                   record)]
      (return
       [:ok record :upserted]))))

(defn delete-stale-secondaries
  [session ^Model model old-record new-record]
  (combine-responses
   (filter
    identity
    (for [t (:secondary-tables model)]
      (let [key (:key t)
            old-key-value (k/extract-key-value key old-record)
            new-key-value (k/extract-key-value key new-record)]
        (when (and old-key-value
                   (not= old-key-value new-key-value))
          (delete-record session
                         model
                         t
                         old-key-value)))))))

(defn upsert-secondaries
  "returns Deferred[Right[]]"
  [session ^Model model record]
  (combine-responses
   (for [t (:secondary-tables model)]
     (let [k (:key t)]
       (when (k/extract-key-value k record)
         (upsert-record session model t record))))))

(defn delete-stale-lookups
  [session ^Model model old-record new-record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-key-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model (or new-record
                                                               old-record))
            key (:key t)
            col-colls (:collections t)]

        (let [old-kvs (set (k/extract-key-value-collection key old-record col-colls))
              new-kvs (set (k/extract-key-value-collection key new-record col-colls))
              stale-kvs (filter identity (set/difference old-kvs new-kvs))]
          (for [kv stale-kvs]
            (delete-record session model t kv))))))))

(defn upsert-lookups
  [session ^Model model record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-key-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model record)
            key (:key t)
            col-colls (:collections t)]
        (let [kvs (filter identity
                          (set (k/extract-key-value-collection key record col-colls)))]
          (for [kv kvs]
            (let [lookup-record (create-lookup-record
                                 uber-key uber-key-value
                                 key kv)]
              (upsert-record session
                             model
                             t
                             lookup-record)))))))))

(defn copy-unique-keys
  [^Model model from to]
  (let [unique-key-tables (:unique-key-tables model)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (assoc r key-col (get from key-col))))
            to
            unique-key-tables)))

(defn upsert
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records
   returns a Deferred[Right[[updated-record key-failures]]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired"

  ([session ^Model model record]

   (let [[record] (t/run-callbacks model :before-save [record])]
     (with-context deferred-context
       (mlet [[old-record
               updated-record-with-keys
               acquire-failures] (unique-key/update-unique-keys
                                  session
                                  model
                                  record)

              stale-secondary-responses (delete-stale-secondaries
                                         session
                                         model
                                         old-record
                                         updated-record-with-keys)

              stale-lookup-responses (delete-stale-lookups
                                      session
                                      model
                                      old-record
                                      updated-record-with-keys)

              secondary-reponses (upsert-secondaries
                                  session
                                  model
                                  updated-record-with-keys)

              lookup-responses (upsert-lookups
                                session
                                model
                                updated-record-with-keys)

              current-record (r/select-one
                              session
                              (get-in model [:primary-table :name])
                              (get-in model [:primary-table :key])
                              (t/extract-uber-key-value model record))]

         (return [current-record
                  acquire-failures]))))))

(defn upsert-many
  "issue one upsert query for each record and combine the responses"
  [session ^Model model records]
  (->> records
       (map (fn [record]
              (upsert session model record)))
       util/combine-responses))
