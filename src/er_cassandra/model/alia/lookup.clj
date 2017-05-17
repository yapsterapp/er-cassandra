(ns er-cassandra.model.alia.lookup
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
    [unique-key :as unique-key]]
   [er-cassandra.model.util.timestamp :as ts]
   [schema.core :as s])
  (:import
   er_cassandra.model.types.Entity
   er_cassandra.session.Session))

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
