(ns er-cassandra.model.alia.relationship
  (:require
   [clojure.set :as set]
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [manifold.deferred :as d]
   [manifold.stream :as st]
   [clj-uuid :as uuid]
   [schema.core :as s]
   [er-cassandra.session]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model :as m]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.error :as e]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.session Session]))

(s/defschema DenormalizeOp
  (s/enum :upsert :delete))

(defn deref-target-entity
  [kw]
  (let [var (ns-resolve (-> kw namespace symbol)
                        (-> kw name symbol))]
    @var))

(s/defn foreign-key-val
  "returns [fk fk-val] for the denorm relationship"
  [source-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema]
  (cond
    (:foreign-key denorm-rel)
    (let [uk-val (t/extract-uber-key-value source-entity source-record)]
      [(:foreign-key denorm-rel) uk-val])

    (:foreign-entity-key denorm-rel)
    (let [ek (-> source-entity :primary-table :entity-key)
          ek-val (k/extract-key-value ek source-record)]
      [(:foreign-entity-key denorm-rel) ek-val])))

(s/defn denormalize-to-target-record
  "denormalize to a single target record"
  [session :- Session
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   target-record :- t/RecordSchema
   denorm-op :- DenormalizeOp
   denorm-version :- s/Uuid]
  (let [[fk fk-val :as fk-vals] (foreign-key-val source-entity source-record denorm-rel)
        fk-map (into {} fk-vals)
        denorm-vals (->> (:denormalize denorm-rel)
                         (map (fn [scol tcol]
                                [tcol (get source-record scol)]))
                         (into {}))
        all-denorm-vals (merge denorm-vals fk-map)
        new-target-record (merge target-record all-denorm-vals)]
    (m/upsert session
              target-entity
              new-target-record)))

(s/defn target-record-stream
  "returns a Deferred<Stream<record>> of target records"
  [session :- Session
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema]
  (let [[fk fk-val] (foreign-key-val source-entity source-record denorm-rel)

        trs (m/select-buffered
             session
             target-entity
             fk-val)]
    (return deferred-context trs)))

(defn only-error
  "given a Deferred keep only errors, otherwise returning Deferred<nil>"
  [dv]
  (-> dv
      (d/chain (constantly nil))
      (d/catch (fn [e] e))))

(s/defn denormalize-rel
  "denormalizes a single relationship"
  [source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   denorm-op :- DenormalizeOp
   denorm-version :- s/Uuid]
  (with-context deferred-context
    (mlet [trs (target-record-stream source-entity
                                     target-entity
                                     source-record
                                     denorm-rel)

           ;; a stream of any errors
           :let [trerrs (->> trs
                             (st/map #(denormalize-to-target-record
                                       source-entity
                                       target-entity
                                       source-record
                                       denorm-rel
                                       %
                                       denorm-op
                                       denorm-version))
                             (st/map only-error)
                             (st/filter identity))]]

      ;; if there are errors, return the first as an exemplar
      (if (st/drained? trerrs)
        (return nil)
        (st/take! trerrs)))))

(s/defn denormalize
  "denormalize all relationships for a given source record"
  [source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-op :- DenormalizeOp]
  (let [rels (-> source-entity :denorm-targets vals)
        v (uuid/v1)
        resps (->> rels
                   (map #(denormalize-rel source-entity
                                          target-entity
                                          source-record
                                          %
                                          denorm-op
                                          v)))]
    (apply d/zip resps)))
