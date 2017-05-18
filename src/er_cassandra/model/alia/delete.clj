(ns er-cassandra.model.alia.delete
  (:require
   [cats.core :as m :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [schema.core :as s]

   [er-cassandra.record :as r]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.fn-schema :as fns]
   [er-cassandra.model.alia.unique-key :as alia-unique-key]
   [er-cassandra.model.alia.upsert :as alia-upsert]
   [er-cassandra.model.alia.select :as alia-select])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.session Session]))

(s/defn delete-primary-record
  "a delete which permits LWTs etc"
  [session :- Session
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

(s/defn ^:private delete-with-primary
  [session :- Session
   entity :- Entity
   key :- t/PrimaryKeySchema
   record :- t/RecordSchema
   opts :- fns/DeleteOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [primary-response (delete-primary-record
                             session
                             entity
                             (:primary-table entity)
                             (t/extract-uber-key-value
                              entity
                              record)
                             opts)

           unique-responses (alia-unique-key/release-stale-unique-keys
                             session
                             entity
                             record
                             nil
                             opts)

           secondary-responses (alia-upsert/delete-stale-secondaries
                                session
                                entity
                                record
                                nil
                                opts)

           lookup-responses (alia-upsert/delete-stale-lookups
                             session
                             entity
                             record
                             nil
                             opts)]
      (return
       [:ok record :deleted]))))

(s/defn delete*
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([session :- Session
    entity :- Entity
    key :- r/KeySchema
    record-or-key-value :- r/RecordOrKeyValueSchema
    opts :- r/DeleteOptsSchema]

   (with-context deferred-context
     (mlet [[record & _] (alia-select/select* session
                                              entity
                                              key
                                              record-or-key-value
                                              nil)
            opts (ts/default-timestamp-opt opts)]
       (if record
         (delete-with-primary
          session
          entity
          (-> entity :primary-table :key)
          record
          opts)
         (return
          [:ok nil :no-primary-record]))))))
