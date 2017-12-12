(ns er-cassandra.model.alia.upsert
  (:require
   [cats
    [context :refer [with-context]]
    [core :as monad :refer [mlet return]]
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
   [er-cassandra.model.alia.delete :as alia.delete]
   [schema.core :as s]
   [prpr.promise :as pr :refer [ddo]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(s/defn insert-index-record
  "insert an index record - doesn't support LWTs, :where etc
   which only apply to the primary record

   weirdly, if a record which was created with update is
   later updated with all null non-pk cols then that record will be
   deleted

   https://ajayaa.github.io/cassandra-difference-between-insert-update/

   since this is undesirable for secondary tables, we use insert instead,
   and since we don't want any :where or LWTs they are forbidden by schema"
  [session :- ModelSession
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

(s/defn upsert-secondaries
  [session :- ModelSession
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

(s/defn upsert-lookups-for-table
  [session :- ModelSession
   entity :- Entity
   table :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [:let [uber-key (t/uber-key entity)
             uber-key-value (t/extract-uber-key-value entity record)]
       lookup-records (->> (l/generate-lookup-records-for-table
                            session entity table old-record record)
                           combine-responses)
       acquire-responses (->> (for [lr lookup-records]
                                (insert-index-record
                                 session
                                 entity
                                 table
                                 lr
                                 opts))
                              combine-responses)]
      (return acquire-responses))))

(s/defn upsert-lookups
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [all-acquire-responses (->> (for [t (t/mutable-lookup-tables entity)]
                                        (upsert-lookups-for-table
                                         session
                                         entity
                                         t
                                         old-record
                                         record
                                         opts))
                                      combine-responses)]
      (return
       (apply concat all-acquire-responses)))))

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

(s/defn update-secondaries-and-lookups
  "update non-LWT secondary and lookup entries"

  [session :- ModelSession
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

           stale-secondary-responses (alia.delete/delete-stale-secondaries
                                      session
                                      entity
                                      old-record
                                      updated-record-with-keys
                                      index-delete-opts)

           stale-lookup-responses (alia.delete/delete-stale-lookups
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

(s/defn upsert-changes*
  "upsert a single instance given the previous value of the instance. if the
   previous value is nil then it's an insert. if the new value is nil then
   it's a delete. otherwise key changes will be computed using the old-record
   and without requiring any select

   returns a Deferred<Pair[record key-failures]> where key-failures describes
   unique keys which were requested but could not be acquired"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]

  (assert (or (nil? old-record)
              (nil? record)
              (= (t/extract-uber-key-value entity old-record)
                 (t/extract-uber-key-value entity record))))

  (ddo [:let [opts (ts/default-timestamp-opt opts)]

        record (t/run-callbacks session entity :before-save record opts)

        [updated-record-with-keys
         acquire-failures] (unique-key/upsert-primary-record-and-update-unique-keys
                            session
                            entity
                            old-record
                            record
                            opts)

        _ (monad/when updated-record-with-keys
            (update-secondaries-and-lookups session
                                            entity
                                            old-record
                                            updated-record-with-keys
                                            opts))

        ;; do any :after-save actions
        final-record (t/run-callbacks
                      session
                      entity
                      :after-save
                      updated-record-with-keys
                      opts)]

    (return
     (pair final-record
           acquire-failures))))

(s/defn upsert*
  "upsert a single instance

   convenience fn - if the entity has any foreign keys it first selects
                    the instance from the db, then calls upsert-changes*"
  [session :- ModelSession
   entity :- Entity
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]
  (ddo [:let [has-foreign-keys? (not-empty
                                 (t/all-foreign-key-cols entity))]

        old-record (monad/when has-foreign-keys?
                     (r/select-one
                      session
                      (get-in entity [:primary-table :name])
                      (get-in entity [:primary-table :key])
                      (t/extract-uber-key-value entity record)))]

    (upsert-changes*
     session
     entity
     old-record
     record
     opts)))

(s/defn change*
  "change a single instance.
   if old-record and record are identical - it's a no-op,
   if record is nil - it's a delete,
   otherwise it's an upsert-changes*

   returns Deferred<[record]>"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]
  (cond
    (nil? record)
    (ddo [dr (alia.delete/delete* session
                                  entity
                                  (t/uber-key entity)
                                  old-record
                                  opts)]
      (return [:delete old-record]))

    (= old-record record)
    (return deferred-context [:noop record])

    :else
    (ddo [[ur] (upsert-changes*
                session
                entity
                old-record
                record
                opts)]
      (return [:upsert ur]))))
