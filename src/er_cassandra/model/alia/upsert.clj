(ns er-cassandra.model.alia.upsert
  (:require
   [cats
    [context :refer [with-context]]
    [core :refer [mlet return]]
    [data :refer [pair]]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.set :as set]
   [er-cassandra
    [key :as k]
    [record :as r]]
   [er-cassandra.model
    [types :as t]
    [util :as util :refer [combine-responses create-lookup-record]]]
   [er-cassandra.model.alia
    [fn-schema :as fns]
    [lookup :as l]
    [unique-key :as unique-key]]
   [er-cassandra.model.util.timestamp :as ts]
   [schema.core :as s])
  (:import
   er_cassandra.model.types.Entity
   er_cassandra.session.Session))

(s/defn delete-index-record
  "delete an index record - doesn't support LWTs, :where etc
   which should only apply to the primary record"
  [session :- Session
   entity :- Entity
   table :- t/TableSchema
   key-value :- t/KeyValueSchema
   opts :- fns/DeleteUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [delete-result (r/delete session
                                   (:name table)
                                   (:key table)
                                   key-value
                                   opts)]
      (return
       [:ok {:table (:name table)
             :key (:key table)
             :key-value key-value} :deleted]))))

(s/defn insert-index-record
  "insert an index record - doesn't support LWTs, :where etc
   which only apply to the primary record

   weirdly, if a record which was created with update is
   later updated with all null non-pk cols then that record will be
   deleted

   https://ajayaa.github.io/cassandra-difference-between-insert-update/

   since this is undesirable for secondary tables, we use insert instead,
   and since we don't want any :where or LWTs they are forbidden by schema"
  [session :- Session
   entity :- Entity
   table :- t/TableSchema
   record :- t/RecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [insert-result (r/insert session
                                   (:name table)
                                   record
                                   opts)]
      (return
       [:ok record :upserted]))))

(s/defn stale-secondary-key-value
  [entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   secondary-table :- t/SecondaryTableSchema]
  (let [key (:key secondary-table)
        old-key-value (k/extract-key-value key old-record)
        new-key-value (k/extract-key-value key new-record)]
    (when (and (k/has-key? key new-record)
               old-key-value
               (not= old-key-value new-key-value))
      old-key-value)))

(s/defn delete-stale-secondaries
  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/DeleteUsingOnlyOptsWithTimestampSchema]
  (combine-responses
   (filter
    identity
    (for [t (t/mutable-secondary-tables entity)]
      (let [stale-key-value (stale-secondary-key-value
                             entity
                             old-record
                             new-record
                             t)]
        ;; (prn "delete-stale-secondaries: " t old-record new-record)
        (if stale-key-value
          (delete-index-record session
                               entity
                               t
                               stale-key-value
                               (fns/upsert-opts->delete-opts opts))
          (return [:ok nil :no-stale-secondary])))))))

(s/defn upsert-secondaries
  [session :- Session
   entity :- Entity
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (combine-responses
   (for [{k :key
          :as t} (t/mutable-secondary-tables entity)]
     (when (and
            (k/has-key? k record)
            (k/extract-key-value k record))
       (insert-index-record session entity t record opts)))))



(s/defn delete-stale-lookups
  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/DeleteUsingOnlyOptsWithTimestampSchema]
  (combine-responses
   (mapcat
    identity
    (for [t (t/mutable-lookup-tables entity)]
      (let [stale-lookups (l/stale-lookup-key-values
                           entity
                           old-record
                           new-record
                           t)]
        (for [skv stale-lookups]
          (delete-index-record session
                               entity
                               t
                               skv
                               (fns/upsert-opts->delete-opts opts))))))))

(s/defn upsert-lookups
  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (combine-responses
   (for [[t r] (l/lookup-record-seq entity old-record record)]
     (insert-index-record session entity t r opts))))

(s/defn copy-unique-keys
  [entity :- Entity
   from :- t/MaybeRecordSchema
   to :- t/MaybeRecordSchema]
  (let [unique-key-tables (:unique-key-tables entity)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (assoc r key-col (get from key-col))))
            to
            unique-key-tables)))

(s/defn has-lookups?
  [entity :- Entity]
  (boolean
   (or (not-empty (t/mutable-secondary-tables entity))
       (not-empty (t/mutable-lookup-tables entity)))))

(s/defn update-secondaries-and-lookups
  "update non-LWT secondary and lookup entries"

  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   updated-record-with-keys :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [:let [index-delete-opts (-> opts
                                       fns/upsert-opts->using-only
                                       fns/upsert-opts->delete-opts)
                 index-insert-opts (-> opts
                                       fns/upsert-opts->using-only)]

           stale-secondary-responses (delete-stale-secondaries
                                      session
                                      entity
                                      old-record
                                      updated-record-with-keys
                                      index-delete-opts)

           stale-lookup-responses (delete-stale-lookups
                                   session
                                   entity
                                   old-record
                                   updated-record-with-keys
                                   index-delete-opts)

           secondary-reponses (upsert-secondaries
                               session
                               entity
                               updated-record-with-keys
                               index-insert-opts)

           lookup-responses (upsert-lookups
                             session
                             entity
                             old-record
                             updated-record-with-keys
                             index-insert-opts)]

      (return updated-record-with-keys))))

(s/defn upsert*
  "upsert a single instance, upserting primary, secondary, unique-key and
   lookup records as required and deleting stale secondary, unique-key and
   lookup records

   returns a Deferred[Pair[updated-record key-failures]] where
   updated-record is the record as currently in the db and key-failures
   is a map of {key values} for unique keys which were requested but
   could not be acquired "
  [session :- Session
   entity :- Entity
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]
   (with-context deferred-context
     (mlet [:let [opts (ts/default-timestamp-opt opts)]

            record (t/run-callbacks entity :before-save record opts)

            ;; don't need the old record if there are no lookups
            old-record (if (has-lookups? entity)
                         (r/select-one
                          session
                          (get-in entity [:primary-table :name])
                          (get-in entity [:primary-table :key])
                          (t/extract-uber-key-value entity record))

                         (return nil))

            [updated-record-with-keys
             acquire-failures] (unique-key/upsert-primary-record-and-update-unique-keys
                                session
                                entity
                                record
                                opts)

            _ (if updated-record-with-keys
                (update-secondaries-and-lookups session
                                                entity
                                                old-record
                                                updated-record-with-keys
                                                opts)
                (return nil))

            ;; re-select the record for nice empty-collections etc
            reselected-record (if updated-record-with-keys
                              (r/select-one
                               session
                               (get-in entity [:primary-table :name])
                               (get-in entity [:primary-table :key])
                               (t/extract-uber-key-value entity record))
                              (return nil))

            ;; gotta make the re-selected record presentable!
            final-record (t/run-callbacks
                          entity
                          :after-load
                          reselected-record)]

       (return
        (pair final-record
              acquire-failures)))))
