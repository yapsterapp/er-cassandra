(ns er-cassandra.model.alia.lookup
  (:require
   [clojure.set :as set]
   [er-cassandra.key :as k]
   [er-cassandra.model
    [types :as t]
    [util :as util :refer [create-lookup-record]]]
   [schema.core :as s]
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.model.types]
   [er-cassandra.model.model-session])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

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
  [entity
   table
   key
   old-record
   record]
  (let [uber-key (t/uber-key entity)
        uber-key-value (or
                        (k/extract-key-value uber-key record)
                        (k/extract-key-value uber-key old-record))
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
                                  entity with-cols old-record record)

                                 lookup-record))]
            lookup-record))))))

(s/defn default-lookup-record-generator-fn
  "generate a sequence of zero or more lookup records for a lookup table
   - model: the model
   - table: the lookup table
   - old-record: previous primary table record (or nil for new)
   - record: upserted primary-table record to generate lookups for
             (or nil for deletion)"
  [session :- ModelSession
   entity :- Entity
   table :- t/IndexTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema]
  (default-lookup-record-generator-for-key-fn
   entity table (:key table) old-record record))

(s/defn generate-lookup-records-for-table
  "generate all the lookup records for one lookup table"
  [session :- ModelSession
   entity :- Entity
   {generator-fn :generator-fn
    :as table} :- t/IndexTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema]
  (if record
    ((or generator-fn
         default-lookup-record-generator-fn)
     session entity table old-record record)
    []))

(s/defn stale-lookup-key-values-for-table
  "returns key-values of stale lookups for a table - to be deleted"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   lookup-table :- t/IndexTableSchema]
  (with-context deferred-context
    ;; only if we are deleting the record, or have sufficient
    ;; key components to update the table
    (let [k (:key lookup-table)]
      (if (or (nil? new-record)
              (k/has-key? k new-record))
        (mlet
          [old-lookups (generate-lookup-records-for-table
                        session entity lookup-table old-record old-record)
           new-lookups (generate-lookup-records-for-table
                        session entity lookup-table old-record new-record)
           :let [old-kvs (set
                          (map #(k/extract-key-value k %)
                               old-lookups))
                 new-kvs (set
                          (map #(k/extract-key-value k %)
                               new-lookups))

                 stale-kvs (filter identity (set/difference old-kvs new-kvs))]]
          (return stale-kvs))
        (return nil)))))

(s/defn insert-and-update-lookup-records-for-table
  "returns lookup records which are to be inserted or require updating
   (because some non-key column was updated)

   have decided not to use this, because i think on balance it's better
   for the collection indexes to be self-healing... i.e. if a collection
   index row wasn't written for some reason then using this would mean
   that it would never get written"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   lookup-table :- t/IndexTableSchema]
  (with-context deferred-context
    (let [k (:key lookup-table)]
      (if (k/has-key? k new-record)
        (mlet
          [old-lookups (generate-lookup-records-for-table
                        session entity lookup-table old-record old-record)
           new-lookups (generate-lookup-records-for-table
                        session entity lookup-table old-record new-record)
           :let [new-kvs (set
                          (map #(k/extract-key-value k %)
                               new-lookups))

                 old-lookups-by-kv (->> old-lookups
                                        (map (fn [l]
                                               [(k/extract-key-value k l) l]))
                                        (into {}))

                 insert-and-update-lookups
                 (->> new-lookups
                      (filter
                       (fn [l]
                         (let [kv (k/extract-key-value k l)
                               old-l (get old-lookups-by-kv kv)]
                           ;; the lookup must be inserted or updated
                           ;; if it's in any way different from the old lookup
                           (not= l old-l)))))]]
          (return insert-and-update-lookups))
        (return nil)))))
