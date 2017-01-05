(ns er-cassandra.model.alia.relationship
  (:require
   [clojure.set :as set]
   [cats.core :refer [mlet return >>=]]
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

(defn- deref-target-entity
  "given a namespace qualififed symbol or keyword referring to a
   var with an Entity, return the Entity... otherwise return whatever
   is given"
  [entity-var-ref]
  (if (or (keyword? entity-var-ref)
          (symbol? entity-var-ref))
    (deref
     (ns-resolve (namespace entity-var-ref) (name entity-var-ref)))

    entity-var-ref))

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
   denorm-op :- DenormalizeOp]
  (let [[fk fk-val :as fk-vals] (foreign-key-val source-entity source-record denorm-rel)
        fk-map (into {} (map vector fk fk-val))
        denorm-vals (->> (:denormalize denorm-rel)
                         (map (fn [[scol tcol]]
                                [tcol (get source-record scol)]))
                         (into {}))
        ]
    (case denorm-op

      :upsert
      (let [all-denorm-vals (merge denorm-vals fk-map)
            new-target-record (merge target-record all-denorm-vals)]
        (m/upsert session
                  target-entity
                  new-target-record))

      :delete
      (let [cascade (:cascade denorm-rel)]
        (case cascade

          :none
          (return deferred-context true)

          :null
          (let [null-denorm-vals (->> denorm-vals
                                      (map (fn [[k v]] [k nil]))
                                      (into {}))]
            (m/upsert session
                      target-entity
                      (merge target-record null-denorm-vals)))

          :delete
          (m/delete session
                    target-entity
                    (-> target-entity :primary-table :key)
                    target-record))))))

(s/defn target-record-stream
  "returns a Deferred<Stream<record>> of target records"
  [session :- Session
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema]
  (with-context deferred-context
    (mlet [:let [[fk fk-val] (foreign-key-val source-entity source-record denorm-rel)]

          trs (m/select-buffered
               session
               target-entity
               fk
               fk-val)]

      (return deferred-context trs))))

(defn only-error
  "given a Deferred keep only errors, otherwise returning Deferred<nil>"
  [dv]
  (-> dv
      (d/chain (constantly nil))
      (d/catch (fn [e] e))))

(s/defn denormalize-rel
  "denormalizes a single relationship"
  [session :- Session
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel-kw :- s/Keyword
   denorm-rel :- t/DenormalizationRelationshipSchema
   denorm-op :- DenormalizeOp
   opts :- {(s/optional-key :buffer-size) s/Int}]
  (with-context deferred-context
    (mlet [trs (target-record-stream session
                                     source-entity
                                     target-entity
                                     source-record
                                     denorm-rel)

           ;; a (hopefully empty) stream of any errors from denormalization
           :let [trerrs (->> trs
                             (st/map #(denormalize-to-target-record
                                       session
                                       source-entity
                                       target-entity
                                       source-record
                                       denorm-rel
                                       %
                                       denorm-op))
                             (st/buffer (or (:buffer-size opts) 25))
                             (st/map only-error)
                             (st/filter identity))]

           maybe-err (st/take! trerrs)]

      ;; if there are errors, return the first as an exemplar
      (if (nil? maybe-err)
        (return [denorm-rel-kw [:ok]])
        (return [denorm-rel-kw [:fail maybe-err]])))))



(s/defn denormalize
  "denormalize all relationships for a given source record
   returns Deferred<[[denorm-rel-kw [status maybe-err]]*]>"
  [session :- Session
   source-entity :- Entity
   source-record :- t/RecordSchema
   denorm-op :- DenormalizeOp
   opts :- {(s/optional-key :buffer-size) s/Int}]
  (let [targets (:denorm-targets source-entity)

        mfs (->> targets
                 (map (fn [[rel-kw rel]]
                        (fn [resps]
                          (with-context deferred-context
                            (mlet [resp (denormalize-rel session
                                                         source-entity
                                                         (deref-target-entity (:target rel))
                                                         source-record
                                                         rel-kw
                                                         rel
                                                         denorm-op
                                                         opts)]
                              (return (conj resps resp))))))))]

    ;; process one relationship at a time, otherwise the buffer-size is
    ;; uncontrolled
    (apply >>= (return deferred-context []) mfs)))
