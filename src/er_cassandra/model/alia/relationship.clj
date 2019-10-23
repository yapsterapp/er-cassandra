(ns er-cassandra.model.alia.relationship
  (:require
   [cats.context :refer [with-context]]
   [cats.core :refer [>>= mlet return]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.set :as set]
   [er-cassandra.model :as m]
   [er-cassandra.model.alia.fn-schema :as fns]
   [er-cassandra.model.types :as t]
   [er-cassandra.record.schema :as rs]
   [manifold.deferred :as d]
   [prpr.promise :as pr :refer [ddo]]
   [prpr.stream :as st]
   [schema.core :as s])
  (:import
   [er_cassandra.model.callbacks.protocol ICallback]
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

(s/defn requires-denorm?
  "true if denorm of the source is required, false if not.
   throws an exception if it's indeterminate.

   denorm is not required if
   - no source columns are present in source-record
   - all source cols present in source-record are unchanged
   denorm is required if
   - source columns are present in source-record with changes
     from old-source-record
   it's indeterminate if
   - source columns are present in source-record with changes
     from old-source-record *but* not all source columns are
     present in source-record/old-source-record"
  [source-col-spec old-source-record source-record]
  (let [source-cols (cond
                      (keyword? source-col-spec) #{source-col-spec}
                      (vector? source-col-spec) (set (first source-col-spec))
                      :else (throw (ex-info "unrecognized source-col-spec"
                                            {:source-col-spec source-col-spec
                                             :old-source-record old-source-record
                                             :source-record source-record})))

        source-col-vals (select-keys source-record source-cols)
        changed-source-cols (if (nil? old-source-record)
                              source-col-vals
                              (reduce
                               (fn [rs [k v]]
                                 (if (and (contains? old-source-record k)
                                          (= v (get old-source-record k)))
                                   rs
                                   (assoc rs k v)))
                               {}
                               source-col-vals))
        missing-source-cols (when-not (or (nil? old-source-record)
                                          (empty? source-col-vals))
                              ;; ^if it's an insert then we can safely use nil
                              ;; values for missing columns but we'll need the
                              ;; either the new or old values for upserts so
                              ;; make sure we've got them
                              (set/difference
                               (set source-cols)
                               (-> old-source-record
                                   (select-keys source-cols)
                                   keys
                                   set
                                   (set/union (set (keys source-col-vals))))))]

    ;; (warn "requires-denorm?" source-col-spec old-source-record source-record)

    (cond
      ;; no source cols present in source-record
      (empty? source-col-vals)
      false

      ;; all source cols are unchanged
      (empty? changed-source-cols)
      false

      ;; source-cols have changed but not *all* source cols are present
      ;; in source-record/old-source-record
      (not-empty missing-source-cols)
      (throw
       (pr/error-ex ::incomplete-source-record
                    {:source-col-spec source-col-spec
                     :old-source-record old-source-record
                     :source-record source-record
                     :missing-source-cols missing-source-cols}))

      ;; some source-cols have changed and nothing's missing
      (and (not-empty changed-source-cols)
           (empty? missing-source-cols))
      true

      :else
      (throw
       (pr/error-ex ::indeterminate-requires-denorm?
                    {:source-col-spec source-col-spec
                     :old-source-record old-source-record
                     :source-record source-record
                     :source-col-vals source-col-vals
                     :changed-source-cols changed-source-cols
                     :missing-source-cols missing-source-cols})))))

(s/defn extract-denorm-source-col
  "extracts a single denorm source column or throws an exception"
  [source-col old-source-record source-record]
  (cond
    (contains? source-record source-col) (get source-record source-col)
    (contains? old-source-record source-col) (get old-source-record source-col)

    ;; requires-denorm? will detect indeterminate updates... if we get to
    ;; here then it's an insertion, with an implicit nil value for the
    ;; source-col in the source-record
    (nil? old-source-record) nil

    :else
    (throw (ex-info "denormalization source col not present"
                          {:source-col source-col
                           :old-source-record old-source-record
                           :source-record source-record}))))

(s/defn derive-denorm-col
  "derives a single target col given the denorm-col-spec"
  [denorm-col-spec old-source-record source-record]
  (cond

    (keyword? denorm-col-spec)
    (extract-denorm-source-col denorm-col-spec old-source-record source-record)

    (vector? denorm-col-spec)
    (let [[source-col-seq denorm-fn] denorm-col-spec
          source-cols (into
                       {}
                       (for [scol source-col-seq]
                         [scol
                          (extract-denorm-source-col
                           scol
                           old-source-record
                           source-record)]))]
      (denorm-fn source-cols))

    :else
    (throw (ex-info
            "unrecognized DenormalizeColumnSchema value"
            {:denorm-col-spec denorm-col-spec
             :old-source-record old-source-record
             :source-record source-record}))))

(s/defn extract-denorm-vals
  "extract the values to be denormalized from a source-record,
   according to the given denorm-rel"
  [{denormalize-spec :denormalize
    :as denorm-rel}
   old-source-record
   source-record]
  (->> denormalize-spec
       (map (fn [[tcol denorm-col-spec]]
              (when (requires-denorm? denorm-col-spec
                                      old-source-record
                                      source-record)
                [tcol
                 (derive-denorm-col
                  denorm-col-spec
                  old-source-record
                  source-record)])))
       (filter identity)
       (into {})
       (not-empty)))

(s/defn denormalize-fields
  "denormalize fields from source-record according to :denormalize
   of denorm-rel, returning an updated target-record. borks if
   denorm fields are not available"
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

        ;; pass a nil old-source-record here, since it's not for
        ;; change requests
        denorm-vals (extract-denorm-vals denorm-rel
                                         nil
                                         source-record)]

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

(s/defn denormalize-fields-to-target
  "if you have a source *and* a target record and you want to denormalize
   any fields that should be denormalised from that source to that target, then
   this is your fn. it will consider all denorm relationships and apply
   denormalize-fields for each relationship where the target entity and
   the source-pk/target-fk values match. only updates in-memory - doesn't
   do any persistence"
  [source-entity :- Entity
   target-entity :- Entity
   source-record :- t/MaybeRecordSchema
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

(s/defn denormalize-to-target-record
  "denormalize to a single target record"
  [session :- ModelSession
   source-entity :- Entity
   target-entity :- Entity
   old-source-record :- t/MaybeRecordSchema
   source-record :- t/MaybeRecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   target-record :- t/RecordSchema
   opts :- fns/DenormalizeOptsSchema]
  (assert (or old-source-record source-record))
  (let [denorm-op (cond
                    (nil? source-record) :delete
                    :else :upsert)

        target-uberkey (-> target-entity :primary-table :key flatten)
        target-uberkey-value (t/extract-uber-key-value
                              target-entity
                              target-record)
        target-uberkey-map (into {} (map vector
                                         target-uberkey
                                         target-uberkey-value))

        [fk fk-val :as fk-vals] (foreign-key-val
                                 source-entity
                                 (or source-record old-source-record)
                                 denorm-rel)
        fk-map (into {} (map vector fk fk-val))]
    ;; (warn "denorm-op" denorm-op)
    (case denorm-op

      ;; we only upsert the uberkey, fk and denormalized cols
      :upsert
      (let [denorm-vals (extract-denorm-vals
                         denorm-rel
                         old-source-record
                         source-record)

            new-target-record (merge denorm-vals
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
          (let [null-denorm-vals (->> denorm-rel
                                      :denormalize
                                      keys
                                      (map (fn [k] [k nil]))
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
   old-source-record :- t/MaybeRecordSchema
   source-record :- t/MaybeRecordSchema
   denorm-rel :- t/DenormalizationRelationshipSchema
   opts :- rs/SelectBufferedOptsSchema]
  (with-context deferred-context
    (mlet [:let [[fk fk-val] (foreign-key-val source-entity
                                              (or source-record
                                                  old-source-record)
                                              denorm-rel)]

          trs (m/select-buffered
               session
               target-entity
               fk
               fk-val
               opts)]

      (return deferred-context trs))))

(s/defn denormalize-rel
  "denormalizes a single relationship"
  [session :- ModelSession
   source-entity :- Entity
   target-entity :- Entity
   old-source-record :- t/MaybeRecordSchema
   source-record :- t/MaybeRecordSchema
   denorm-rel-kw :- s/Keyword
   denorm-rel :- t/DenormalizationRelationshipSchema
   {buffer-size :buffer-size :as opts} :- fns/DenormalizeOptsSchema]
  (let [dvs  (when (some? source-record)
               (extract-denorm-vals denorm-rel old-source-record source-record))]
    ;; (warn "denormalize-rel" {:dvs dvs})
    (if (and (some? source-record)
             (empty? dvs))
      ;; don't do anything if we don't need to
      (return deferred-context [denorm-rel-kw :noop])

      (with-context deferred-context
        (mlet [trs (target-record-stream session
                                         source-entity
                                         target-entity
                                         old-source-record
                                         source-record
                                         denorm-rel
                                         (select-keys opts [:fetch-size]))

               ;; a (hopefully empty) stream of any errors from denormalization
               :let [denorms (->> trs
                                  (st/map-concurrently
                                   (or buffer-size 25)
                                   #(denormalize-to-target-record
                                     session
                                     source-entity
                                     target-entity
                                     old-source-record
                                     source-record
                                     denorm-rel
                                     %
                                     opts)))]

               ;; consumes the whole stream, returns the first error
               ;; or nil if no errors
               maybe-err (d/catch
                          (st/reduce-all-throw
                           ::denormalize-rel
                           (constantly nil)
                           nil ;; needed in case the stream only has a single value
                           denorms)
                          identity)]

          ;; if there are errors, return the first as an exemplar
          (if (nil? maybe-err)
            (return [denorm-rel-kw [:ok]])
            (return [denorm-rel-kw [:fail maybe-err]])))))))

(s/defn denormalize
  "denormalize all relationships for a given source record
   returns Deferred<[[denorm-rel-kw [status maybe-err]]*]>"
  [session :- ModelSession
   source-entity :- Entity
   old-source-record :- t/MaybeRecordSchema
   source-record :- t/MaybeRecordSchema
   {skip-denorm-rels ::t/skip-denormalize
    :as opts} :- fns/DenormalizeOptsSchema]
  (let [opts (dissoc opts ::t/skip-denormalize)
        targets (:denorm-targets source-entity)
        target-kw-set (->> targets (map first) set)
        skip-denorm-rels-set (set skip-denorm-rels)

        bad-skips (set/difference skip-denorm-rels-set
                                  (conj target-kw-set
                                        ::t/skip-all-denormalize))

        _ (when (not-empty bad-skips)
            (throw (pr/error-ex
                    ::denormalize-bad-skip
                    {:source-entity (-> source-entity :primary-table :name)
                     :old-source-record old-source-record
                     :source-record source-record
                     :opts opts
                     :bad-skips bad-skips})))

          mfs (->> targets
                   (map (fn [[rel-kw rel]]
                          (fn [resps]
                            (if (or
                                 (contains? skip-denorm-rels
                                            ::t/skip-all-denormalize)
                                 (contains? skip-denorm-rels
                                            rel-kw))
                              (return
                               deferred-context
                               (conj resps [rel-kw [:skip]]))
                              (ddo [resp (denormalize-rel
                                          session
                                          source-entity
                                          (deref-target-entity (:target rel))
                                          old-source-record
                                          source-record
                                          rel-kw
                                          rel
                                          opts)]
                                (return (conj resps resp))))))))]

      ;; process one relationship at a time, otherwise the buffer-size is
      ;; uncontrolled
    (apply >>= (return deferred-context []) mfs)))

(s/defn denormalize-callback
  "creates a denormalize callback for :after-save and/or :after-delete"
  ([] (denormalize-callback {}))
  ([denorm-opts :- fns/DenormalizeCallbackOptsSchema]
   (reify
     ICallback
     (-after-save [_ session entity old-record record opts]
       ;; (warn "denormalize-callback" old-record record)
       (denormalize
        session
        entity
        old-record
        record
        (merge
         (fns/upsert-opts->denormalize-opts opts)
         denorm-opts)))

     (-after-delete [_ session entity record opts]
       (denormalize
        session
        entity
        record
        nil
        (merge
         (fns/delete-opts->denormalize-opts opts)
         denorm-opts))))))
