(ns er-cassandra.model.upsert
  (:require
   [clojure.set :as set]
   [clojure.core.match :refer [match]]
   [manifold.deferred :as d]
   [cats.core :as m]
   [cats.monad.either :as either]
   [cats.monad.deferred :as dm]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.key :as k]
   [er-cassandra.record-either :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]]
   [er-cassandra.model.unique-key :as uk])
  (:import [er_cassandra.model.types Model]))

(defn delete-record
  [session model table key-value]
  (m/with-monad dm/either-deferred-monad
    (m/mlet [delete-result (r/delete session
                                      (:name table)
                                      (:key table)
                                      key-value)]
            (m/return
             [:ok {:table (:name table)
                   :key (:key table)
                   :key-value key-value} :deleted]))))

(defn upsert-record
  [session ^Model model table record]
  (m/with-monad dm/either-deferred-monad
    (m/mlet [insert-result (r/insert session
                                      (:name table)
                                      record)]
            (m/return
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
     (upsert-record session model t record))))

(defn delete-stale-lookups
  [session ^Model model old-record new-record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model new-record)
            key (:key t)
            coll (:collection t)]

        (if coll
          (let [old-kvs (set (k/extract-key-value-collection key old-record))
                new-kvs (set (k/extract-key-value-collection key new-record))
                stale-kvs (filter identity (set/difference new-kvs old-kvs))]
            (for [kv stale-kvs]
              (delete-record session model t kv)))

          (let [old-kv (k/extract-key-value key old-record)
                new-kv (k/extract-key-value key new-record)]
            (when (and old-kv
                       (not= old-kv new-kv))
              [(delete-record session model t old-kv)]))))))))

(defn upsert-lookups
  [session ^Model model record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model record)
            key (:key t)
            coll (:collection t)]
        (if coll
          (let [kvs (filter identity
                            (set (k/extract-key-value-collection key record)))]
            (for [kv kvs]
              (let [lookup-record (create-lookup-record
                                   uber-key uber-key-value
                                   key kv)]
                (upsert-record session
                               model
                               t
                               lookup-record))))

          (when-let [key-value (k/extract-key-value key record)]
            (let [lookup-record (create-lookup-record
                                 uber-key uber-key-value
                                 key key-value)]
              [(upsert-record session model t lookup-record)]))))))))

(defn upsert
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records"

  ([session ^Model model record]

   (m/with-monad dm/either-deferred-monad
     (m/mlet [with-unique-keys (uk/acquire-unique-keys session model record)

              stale-secondary-responses (delete-stale-secondaries
                                         session
                                         model
                                         with-unique-keys
                                         record)

              stale-lookup-responses (delete-stale-lookups
                                      session
                                      model
                                      with-unique-keys
                                      record)

              secondary-reponses (combine-responses
                                  (upsert-secondaries
                                   session
                                   model
                                   record))

              lookup-responses (combine-responses
                                (upsert-lookups
                                 session
                                 model
                                 record))]
             (m/return
              (r/select-one session
                             (get-in model [:primary-table :name])
                             (get-in model [:primary-table :key])
                             (t/extract-uber-key-value model record)))))))
