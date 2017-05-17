(ns er-cassandra.model.alia.upsert
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.set :as set]
   [schema.core :as s]
   [manifold.deferred :as d]
   [cats.core :refer [mlet return]]
   [cats.data :refer [pair]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.util :as util
    :refer [combine-responses create-lookup-record]]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.alia.fn-schema :as fns]
   [er-cassandra.model.alia.unique-key :as unique-key]
   [er-cassandra.model.error :as e])
  (:import
   [er_cassandra.session Session]
   [er_cassandra.model.types Entity]))

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

(defn choose-lookup-additional-cols
  "we want to choose a minimum set of additional cols, over the
   lookup-pk + uber-key...

   we default with-cols values from old record
   otherwise MVs depending on the lookup may
   have rows removed because with-cols cols
   weren't supplied "
  [model with-cols old-record record]
  (let [with-cols (if (= :all with-cols)
                    (set/union (set (keys old-record))
                               (set (keys record)))
                    with-cols)]
    (merge
     (select-keys old-record with-cols)
     (select-keys record with-cols))))

(s/defn default-lookup-record-generator-for-key-fn
  "generate lookup record(s) for a lookup table given
   a specified key which may or may not be the :key of
   the lookup-table
   - model: the model
   - table: the lookup table
   - key : the key to use to generate lookups
   - old-record: previous primary table record (or nil for new)
   - record: upserted primary-table record to generate lookups for
             (or nil for deletion)"
  [model
   table
   key
   old-record
   record]
  (let [uber-key (t/uber-key model)
        uber-key-value (t/extract-uber-key-value model record)
        col-colls (:collections table)
        with-cols (:with-columns table)]
    (when (k/has-key? key record)
      (let [kvs (filter identity
                        (set (k/extract-key-value-collection
                              key record col-colls)))]

        (for [kv kvs]
          (let [lookup-record (create-lookup-record
                               uber-key uber-key-value key kv)

                lookup-record (if-not with-cols

                                lookup-record

                                (merge
                                 (choose-lookup-additional-cols
                                  model with-cols old-record record)

                                 lookup-record))]
            lookup-record))))))

(s/defn default-lookup-record-generator-fn
  "generate a sequence of zero or more lookup records for a lookup table
   - model: the model
   - table: the lookup table
   - old-record: previous primary table record (or nil for new)
   - record: upserted primary-table record to generate lookups for
             (or nil for deletion)"
  [model :- Entity
   table :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema]
  (if record
    (default-lookup-record-generator-for-key-fn
     model table (:key table) old-record record)
    []))

(s/defn generate-lookup-records-for-table
  "generate all the lookup records for one lookup table"
  [model :- Entity
   {generator-fn :generator-fn
    :as table} :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema]
  ((or generator-fn
       default-lookup-record-generator-fn)
   model table old-record record))

(s/defn lookup-record-seq
  "returns a seq of tuples [table lookup-record]"
  [model :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema]
  (apply
   concat
   (for [t (t/mutable-lookup-tables model)]
     (->> (generate-lookup-records-for-table
           model t old-record record)
          (map (fn [lr]
                 [t lr]))))))

(s/defn stale-lookup-key-values
  [entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   lookup-key-table :- t/LookupTableSchema]
  (let [key (:key lookup-key-table)
        col-colls (:collections lookup-key-table)]

    (when (k/has-key? key new-record)
      ;; get old lookup keys which aren't present in the latest
      ;; set of lookup records
      (let [old-kvs (set
                     (map #(k/extract-key-value key %)
                          (generate-lookup-records-for-table
                           entity lookup-key-table old-record old-record)))
            new-kvs (set
                     (map #(k/extract-key-value key %)
                          (generate-lookup-records-for-table
                           entity lookup-key-table old-record new-record)))]
        (filter identity (set/difference old-kvs new-kvs))))))

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
      (let [stale-lookups (stale-lookup-key-values
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
   (for [[t r] (lookup-record-seq entity old-record record)]
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
