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
   [er-cassandra.util.stream :as stu]
   [er-cassandra.model :as m]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.error :as e]
   [er-cassandra.model.alia.fn-schema :as fns]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]]
   [taoensso.timbre :refer [info warn]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

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
  (let [uk-val (t/extract-uber-key-value source-entity source-record)]
    [(:foreign-key denorm-rel) uk-val]))

(s/defn denormalize-fields
  "denormalize fields from source-record according to :denormalize
   of denorm-rel, returning an updated target-record"
  [source-entity :- Entity
   target-entity :- Entity
   denorm-rel :- t/DenormalizationRelationshipSchema
   source-record :- t/RecordSchema
   target-record :- t/RecordSchema]
  (let [target-uberkey (-> target-entity :primary-table :key flatten)
        target-uberkey-value (t/extract-uber-key-value
                              target-entity
                              target-record)
        target-uberkey-map (into {} (map vector
                                         target-uberkey
                                         target-uberkey-value))

        [fk fk-val] (foreign-key-val source-entity source-record denorm-rel)
        fk-map (into {} (map vector fk fk-val))

        denorm-vals (->> (:denormalize denorm-rel)
                         (map (fn [[tcol scol-or-fn]]
                                [tcol (scol-or-fn source-record)]))
                         (into {}))]
    ;; the merge ensures that a bad denorm spec can't
    ;; change the PK/FK of the target
    (merge target-record
           denorm-vals
           fk-map
           target-uberkey-map)))

(s/defn matching-rels
  "given a source-record and a target-record and their entities, return
   any denorm-rels from the source-entity which are applicable to the pair,
   by matching the target-entity and the foreign keys"
  [source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   target-record :- t/RecordSchema]
  (->> (for [[rel-kw {rel-target-ref :target :as rel}]
             (:denorm-targets source-entity)]
         (let [rel-target-entity (deref-target-entity rel-target-ref)]
           (assoc rel :target rel-target-entity)))

       (filter (fn [{rel-target :target}]
                 (= rel-target target-entity)))

       (filter (fn [rel]
                 (let [[fk fk-val] (foreign-key-val source-entity
                                                    source-record
                                                    rel)]
                   (= ((apply juxt fk) target-record)
                      fk-val))))))

(s/defn denormalize-to-target-record
  "denormalize to a single target record"
  [session :- ModelSession
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   target-record :- t/RecordSchema
   denorm-op :- DenormalizeOp
   opts :- fns/DenormalizeOptsSchema]
  (let [target-uberkey (-> target-entity :primary-table :key flatten)
        target-uberkey-value (t/extract-uber-key-value
                              target-entity
                              target-record)
        target-uberkey-map (into {} (map vector
                                         target-uberkey
                                         target-uberkey-value))

        [fk fk-val :as fk-vals] (foreign-key-val source-entity source-record denorm-rel)
        fk-map (into {} (map vector fk fk-val))

        denorm-vals (->> (:denormalize denorm-rel)
                         (map (fn [[tcol scol-or-fn]]
                                [tcol (scol-or-fn source-record)]))
                         (into {}))]

    (case denorm-op

      ;; we only upsert the uberkey, fk and denormalized cols
      :upsert
      (let [new-target-record (merge denorm-vals
                                     fk-map
                                     target-uberkey-map)
            otr (select-keys
                 target-record
                 (keys new-target-record))]
        ;; change only changes if necessary
        (m/change session
                  target-entity
                  otr
                  new-target-record
                  (fns/denormalize-opts->upsert-opts opts)))

      :delete
      (let [cascade (:cascade denorm-rel)]
        (case cascade

          :none
          (return deferred-context true)

          ;; we only upsert the uberkey, fk and denormalized cols
          :null
          (let [null-denorm-vals (->> denorm-vals
                                      (map (fn [[k v]] [k nil]))
                                      (into {}))
                new-target-record (merge null-denorm-vals
                                         fk-map
                                         target-uberkey-map)
                otr (select-keys
                     target-record
                     (keys new-target-record))]
            (m/change session
                      target-entity
                      otr
                      new-target-record
                      (fns/denormalize-opts->upsert-opts opts)))

          :delete
          (m/delete session
                    target-entity
                    (-> target-entity :primary-table :key)
                    target-record
                    (fns/denormalize-opts->delete-opts opts)))))))

(s/defn target-record-stream
  "returns a Deferred<Stream<record>> of target records"
  [session :- ModelSession
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   opts :- r/SelectBufferedOptsSchema]
  (with-context deferred-context
    (mlet [:let [[fk fk-val] (foreign-key-val source-entity source-record denorm-rel)]

          trs (m/select-buffered
               session
               target-entity
               fk
               fk-val
               opts)]

      (return deferred-context trs))))

(defn only-error
  "given a Deferred keep only errors, otherwise returning Deferred<nil>"
  [dv]
  (-> dv
      (d/chain (fn [v] (when (instance? Throwable v) v)))))

(s/defn denormalize-rel
  "denormalizes a single relationship"
  [session :- ModelSession
   source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   denorm-rel-kw :- s/Keyword
   denorm-rel :- t/DenormalizationRelationshipSchema
   denorm-op :- DenormalizeOp
   opts :- fns/DenormalizeOptsSchema]
  (with-context deferred-context
    (mlet [trs (target-record-stream session
                                     source-entity
                                     target-entity
                                     source-record
                                     denorm-rel
                                     (select-keys opts [:fetch-size]))

           ;; a (hopefully empty) stream of any errors from denormalization
           :let [denorms (->> trs
                              (st/map #(denormalize-to-target-record
                                        session
                                        source-entity
                                        target-entity
                                        source-record
                                        denorm-rel
                                        %
                                        denorm-op
                                        opts))
                              (st/buffer (or (:buffer-size opts) 25)))]

           ;; consumes the whole stream, returns the first error
           ;; or nil if no errors
           maybe-err (stu/keep-stream-error denorms)]

      ;; if there are errors, return the first as an exemplar
      (if (nil? maybe-err)
        (return [denorm-rel-kw [:ok]])
        (return [denorm-rel-kw [:fail maybe-err]])))))

(s/defn denormalize-fields-to-target
  "if you have a source *and* a target record and you want to denormalize
   any fields that should be denormalised from that source to that target, then
   this is your fn. it will consider all denorm relationships and apply
   denormalize-fields for each relationship where the target entity and
   the source-pk/target-fk values match"
  [source-entity :- Entity
   target-entity :- Entity
   source-record :- t/RecordSchema
   target-record :- t/RecordSchema]
  (let [denorm-rels (matching-rels
                     source-entity
                     target-entity
                     source-record
                     target-record)]
    (reduce (fn [tr rel]
              (denormalize-fields
               source-entity
               target-entity
               rel
               source-record
               tr))
            target-record
            denorm-rels)))

(s/defn denormalize
  "denormalize all relationships for a given source record
   returns Deferred<[[denorm-rel-kw [status maybe-err]]*]>"
  [session :- ModelSession
   source-entity :- Entity
   source-record :- t/RecordSchema
   denorm-op :- DenormalizeOp
   opts :- fns/DenormalizeOptsSchema]
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

(s/defn denormalize-callback
  "creates a callback with the given op and opts"
  ([denorm-op] (denormalize-callback denorm-op {}))
  ([denorm-op :- DenormalizeOp
    denorm-opts :- fns/DenormalizeCallbackOptsSchema]
   (reify
     t/ICallback
     (-run-callback [_ session entity record opts]
       (denormalize
        session
        entity
        record
        denorm-op
        (merge opts denorm-opts))))))
