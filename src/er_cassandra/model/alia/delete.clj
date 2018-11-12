(ns er-cassandra.model.alia.delete
  (:require
   [cats.core :as m :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [schema.core :as s]
   [er-cassandra
    [key :as k]
    [record :as r]]
   [er-cassandra.record.schema :as rs]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model
    [callbacks :as cb]
    [types :as t]
    [util :as util :refer [combine-responses create-lookup-record]]]
   [er-cassandra.model.alia
    [fn-schema :as fns]
    [lookup :as l]
    [unique-key :as unique-key]]
   [er-cassandra.model.alia.select :as alia-select]
   [prpr.promise :refer [ddo]]
   [er-cassandra.model.model-session :as ms])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(s/defn delete-primary-record
  "a delete which permits LWTs etc"
  [session :- ModelSession
   entity :- Entity
   table :- t/TableSchema
   key-value :- t/KeyValueSchema
   opts :- fns/DeleteOptsWithTimestampSchema]
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

(defn nil-values
  "return a record with the same keys as m but nil values... used to
   force all secondary/lookup keys to be considered for stale deletion"
  [m]
  (->> m
       keys
       (map (fn [k] [k nil]))
       (into {})))

(s/defn delete-index-record
  "delete an index record - doesn't support LWTs, :where etc
   which should only apply to the primary record"
  [session :- ModelSession
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

(s/defn stale-secondary-key-value
  [entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   secondary-table :- t/SecondaryTableSchema]
  (let [key (:key secondary-table)
        old-key-value (k/extract-key-value key old-record)
        new-key-value (k/extract-key-value key new-record)]
    (when (and (or (nil? new-record) (k/has-key? key new-record))
               old-key-value
               (not= old-key-value new-key-value))
      old-key-value)))

(s/defn delete-stale-secondaries
  [session :- ModelSession
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

(s/defn delete-stale-lookups-for-table
  [session :- ModelSession
   entity :- Entity
   table :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/DeleteUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [stale-lookups (l/stale-lookup-key-values-for-table
                      session
                      entity
                      old-record
                      new-record
                      table)]
      (combine-responses
       (for [skv stale-lookups]
         (delete-index-record session
                              entity
                              table
                              skv
                              (fns/upsert-opts->delete-opts opts)))))))

(s/defn delete-stale-lookups
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/DeleteUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [all-delete-responses (->> (for [t (t/mutable-lookup-tables entity)]
                                   (delete-stale-lookups-for-table
                                    session
                                    entity
                                    t
                                    old-record
                                    new-record
                                    opts))
                                 combine-responses)]
      (return
       (apply concat all-delete-responses)))))

(s/defn ^:private delete-with-primary
  [session :- ModelSession
   entity :- Entity
   key :- t/PrimaryKeySchema
   record :- t/RecordSchema
   opts :- fns/DeleteOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [_ (cb/run-callbacks
              session
              entity
              :before-delete
              record
              opts)

           primary-response (delete-primary-record
                             session
                             entity
                             (:primary-table entity)
                             (t/extract-uber-key-value
                              entity
                              record)
                             opts)

           unique-responses (unique-key/change-unique-keys
                             session
                             entity
                             record
                             nil
                             opts)

           secondary-responses (delete-stale-secondaries
                                session
                                entity
                                record
                                nil
                                opts)

           lookup-responses (delete-stale-lookups
                             session
                             entity
                             record
                             nil
                             opts)

           _ (cb/run-callbacks
              session
              entity
              :after-delete
              record
              opts)]
      (return
       [:ok record :deleted]))))

(s/defn delete*
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([session :- ModelSession
    entity :- Entity
    key :- rs/KeySchema
    record-or-key-value :- rs/RecordOrKeyValueSchema
    opts :- rs/DeleteOptsSchema]

   (ddo [;; if it's a record, don't re-select
         [record & _] (if (t/satisfies-entity? entity record-or-key-value)
                        (return [record-or-key-value])
                        (alia-select/select* session
                                             entity
                                             key
                                             record-or-key-value
                                             (fns/opts->prepare?-opt
                                              opts)))
         opts (ts/default-timestamp-opt opts)]

     (if record
       (delete-with-primary
        session
        entity
        (-> entity :primary-table :key)
        record
        opts)
       (return
        [:ok nil :no-primary-record])))))
