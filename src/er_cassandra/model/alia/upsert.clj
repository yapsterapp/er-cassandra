(ns er-cassandra.model.alia.upsert
  (:require
   [clojure.set :as set]
   [manifold.deferred :as d]
   [cats.core :refer [mlet return]]
   [cats.data :refer [pair]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.session :as s]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.util :as util
    :refer [combine-responses create-lookup-record]]
   [er-cassandra.model.alia.unique-key :as unique-key])
  (:import
   [er_cassandra.session Session]
   [er_cassandra.model.types Model]))

(defn delete-record
  [^Session session ^Model model table key-value]
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
  [^Session session ^Model model table record]
  (with-context deferred-context
    (mlet [insert-result (r/insert session
                                   (:name table)
                                   record)]
      (return
       [:ok record :upserted]))))

(defn stale-secondary-key-value
  [^Model model old-record new-record secondary-table]
  (let [key (:key secondary-table)
        old-key-value (k/extract-key-value key old-record)
        new-key-value (k/extract-key-value key new-record)]
    (when (and (k/has-key? key new-record)
               old-key-value
               (not= old-key-value new-key-value))
      old-key-value)))

(defn delete-stale-secondaries
  [^Session session ^Model model old-record new-record]
  (combine-responses
   (filter
    identity
    (for [t (:secondary-tables model)]
      (let [stale-key-value (stale-secondary-key-value
                             model
                             old-record
                             new-record
                             t)]
        (if stale-key-value
          (delete-record session
                         model
                         t
                         stale-key-value)
          (return [:ok nil :no-stale-secondary])))))))

(defn upsert-secondaries
  "returns Deferred[Right[]]"
  [^Session session ^Model model record]
  (combine-responses
   (for [t (:secondary-tables model)]
     (let [k (:key t)]
       (when (and
              (k/has-key? k record)
              (k/extract-key-value k record))
         (upsert-record session model t record))))))

(defn stale-lookup-key-values
  [^Model model old-record new-record lookup-key-table]
  (let [key (:key lookup-key-table)
        col-colls (:collections lookup-key-table)]

    (when (k/has-key? key new-record)
      (let [old-kvs (set (k/extract-key-value-collection key old-record col-colls))
            new-kvs (set (k/extract-key-value-collection key new-record col-colls))]
        (filter identity (set/difference old-kvs new-kvs))))))

(defn delete-stale-lookups
  [^Session session ^Model model old-record new-record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-key-tables model)]
      (let [stale-kvs (stale-lookup-key-values model old-record new-record t)]

        (for [kv stale-kvs]
          (delete-record session model t kv)))))))

(defn upsert-lookups
  [^Session session ^Model model record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-key-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model record)
            key (:key t)
            col-colls (:collections t)]
        (when (k/has-key? key record)
          (let [kvs (filter identity
                            (set (k/extract-key-value-collection key record col-colls)))]
            (for [kv kvs]
              (let [lookup-record (create-lookup-record
                                   uber-key uber-key-value
                                   key kv)]
                (upsert-record session
                               model
                               t
                               lookup-record))))))))))

(defn copy-unique-keys
  [^Model model from to]
  (let [unique-key-tables (:unique-key-tables model)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (assoc r key-col (get from key-col))))
            to
            unique-key-tables)))

(defn has-lookups?
  [^Model model]
  (boolean
   (or (not-empty (:secondary-tables model))
       (not-empty (:lookup-key-tables model)))))

(defn update-secondaries-and-lookups
  "update non-LWT secondary and lookup entries"

  ([^Session session ^Model model old-record updated-record-with-keys]
   (with-context deferred-context
     (mlet [stale-secondary-responses (delete-stale-secondaries
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
                              updated-record-with-keys)]

       (return updated-record-with-keys)))))

(defn upsert*
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records

   returns a Deferred[Pair[updated-record key-failures]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired "
  [^Session session ^Model model record opts]
   (with-context deferred-context
     (mlet [:let [[record] (t/run-callbacks model :before-save [record])]

            ;; don't need the old record if there are no lookups
            old-record (if (has-lookups? model)
                         (r/select-one
                          session
                          (get-in model [:primary-table :name])
                          (get-in model [:primary-table :key])
                          (t/extract-uber-key-value model record))

                         (return nil))

            [updated-record-with-keys
             acquire-failures] (unique-key/upsert-primary-record-and-update-unique-keys
                                session
                                model
                                record
                                opts)

            _ (if updated-record-with-keys
                (update-secondaries-and-lookups session
                                                model
                                                old-record
                                                updated-record-with-keys)
                (return nil))

            ;; re-select the record for nice empty-collections etc
            final-record (if updated-record-with-keys
                           (r/select-one
                            session
                            (get-in model [:primary-table :name])
                            (get-in model [:primary-table :key])
                            (t/extract-uber-key-value model record))
                           (return nil))]

       (return
        (pair final-record
              acquire-failures)))))
