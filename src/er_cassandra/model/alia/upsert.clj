(ns er-cassandra.model.alia.upsert
  (:require
   [cats.context :refer [with-context]]
   [cats.core :as monad :refer [mlet return]]
   [cats.data :refer [pair]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.pprint :refer [pprint]]
   [clojure.set :as set]
   [er-cassandra.key :as k]
   [er-cassandra.model.alia.delete :as alia.delete]
   [er-cassandra.model.alia.fn-schema :as fns]
   [er-cassandra.model.alia.lookup :as lookup]
   [er-cassandra.model.alia.minimal-change :as min.ch]
   [er-cassandra.model.alia.unique-key :as unique-key]
   [er-cassandra.model.callbacks :as cb]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.record :as r]
   [prpr.promise :as pr :refer [ddo]]
   [prpr.stream :as stream]
   [schema.core :as s])
  (:import
   [er_cassandra.model.model_session ModelSession]
   [er_cassandra.model.types Entity]))

(s/defn upsert-index-record
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
   {versioned? :versioned?
    :as table} :- t/TableSchema
   record :- t/RecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [:let [has-collection-cols? (some
                                       (comp t/is-collection-column-diff? second)
                                       record)]
           insert-result (if (and (not versioned?) has-collection-cols?)
                           (r/update
                            session
                            (:name table)
                            (:key table)
                            record
                            opts)
                           (r/insert
                            session
                            (:name table)
                            record
                            opts))]
      (return
       [:upserted record]))))

(s/defn change-secondary
  [session :- ModelSession
   entity :- Entity
   {t-k :key
    :as table} :- t/SecondaryTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   {minimal-change? ::t/minimal-change
    :as opts} :- fns/UpsertOptsWithTimestampSchema
   [old-secondary-record
    new-secondary-record
    :as secondary-change] :- t/ChangeSchema]

  (cond
    (and (nil? old-secondary-record)
         (nil? new-secondary-record))
    (throw
     (pr/error-ex ::secondary-change-both-nil
                  {:entity entity
                   :table table
                   :old-record old-record
                   :new-record new-record
                   :old-secondary-record old-secondary-record
                   :new-secondary-record new-secondary-record
                   :change secondary-change}))

    (nil? new-secondary-record)
    (ddo [:let [kv (k/extract-key-value t-k old-secondary-record)]
          [_ dr] (alia.delete/delete-index-record
                  session
                  entity
                  table
                  kv
                  (fns/primary-upsert-opts->lookup-delete-opts opts))]
      (return
       [:deleted dr]))

    (nil? old-secondary-record)
    (ddo [:let [min-secondary-change (min.ch/avoid-tombstone-change-for-table
                                      table
                                      old-secondary-record
                                      new-secondary-record)]

          _ (upsert-index-record
             session
             entity
             table
             min-secondary-change
             (fns/primary-upsert-opts->lookup-upsert-opts opts))]
      (return
       [:upserted new-secondary-record]))

    :else
    (ddo [:let [min-secondary-change ((if minimal-change?
                                        min.ch/minimal-change-for-table
                                        min.ch/avoid-tombstone-change-for-table)
                                      table
                                      old-secondary-record
                                      new-secondary-record)]

          _ (monad/when min-secondary-change
              (upsert-index-record
               session
               entity
               table
               min-secondary-change
               (fns/primary-upsert-opts->lookup-upsert-opts opts)))]
      (if min-secondary-change
        (return [:upserted new-secondary-record])
        (return [:nochange new-secondary-record])))))

(s/defn change-secondaries-for-table
  [session :- ModelSession
   entity :- Entity
   table :- t/SecondaryTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (ddo [s-changes (lookup/generate-secondary-changes-for-table
                   session
                   entity
                   table
                   old-record
                   new-record)]
    (if s-changes
      (->> s-changes
           (stream/->source)
           (stream/map (partial
                        change-secondary
                        session
                        entity
                        table
                        old-record
                        new-record
                        opts))
           (stream/realize-each)
           (stream/reduce conj []))
      (return []))))

(s/defn change-secondaries
  "insert a minimal change for each secondary"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (->> (t/mutable-secondary-tables entity)
       (stream/->source)
       (stream/map #(change-secondaries-for-table
                     session
                     entity
                     %
                     old-record
                     new-record
                     opts))
       (stream/realize-each)
       (stream/reduce into [])))

(s/defn change-lookup
  [session :- ModelSession
   entity :- Entity
   {t-k :key
    :as table} :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   {minimal-change? ::t/minimal-change
    :as opts} :- fns/UpsertOptsWithTimestampSchema
   [old-lookup-record
    new-lookup-record
    :as lookup-change] :- t/ChangeSchema]

  (cond
    (and (nil? old-lookup-record)
         (nil? new-lookup-record))
    (throw
     (pr/error-ex ::lookup-change-both-nil
                  {:entity entity
                   :table table
                   :old-record old-record
                   :new-record new-record
                   :change lookup-change}))

    (nil? new-lookup-record)
    (ddo [:let [kv (k/extract-key-value t-k old-lookup-record)]
          [_ dr] (alia.delete/delete-index-record
                  session
                  entity
                  table
                  kv
                  (fns/primary-upsert-opts->lookup-delete-opts opts))]
      (return
       [:deleted dr]))

    (nil? old-lookup-record)
    (ddo [:let [min-change (min.ch/avoid-tombstone-change-for-table
                            table
                            nil
                            new-lookup-record)]
          _ (upsert-index-record
             session
             entity
             table
             new-lookup-record
             (fns/primary-upsert-opts->lookup-upsert-opts opts))]
      (return [:upserted new-lookup-record]))

    :else
    (ddo [:let [min-change ((if minimal-change?
                              min.ch/minimal-change-for-table
                              min.ch/avoid-tombstone-change-for-table)
                            table
                            old-lookup-record
                            new-lookup-record)]
          _ (monad/when min-change
              (upsert-index-record
               session
               entity
               table
               min-change
               (fns/primary-upsert-opts->lookup-upsert-opts opts)))]
      (if min-change
        (return [:upserted new-lookup-record])
        (return [:nochange new-lookup-record])))))

(s/defn change-lookups-for-table
  [session :- ModelSession
   entity :- Entity
   table :- t/LookupTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (ddo [l-changes (lookup/generate-lookup-changes-for-table
                   session
                   entity
                   table
                   old-record
                   new-record)]
    (if l-changes
      (->> l-changes
           (stream/->source)
           (stream/map (partial
                        change-lookup
                        session
                        entity
                        table
                        old-record
                        new-record
                        opts))
           (stream/realize-each)
           (stream/reduce conj []))
      (return []))))

(s/defn change-lookups
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (->> (t/mutable-lookup-tables entity)
       (stream/->source)
       (stream/map #(change-lookups-for-table
                     session
                     entity
                     %
                     old-record
                     new-record
                     opts))
       (stream/realize-each)
       (stream/reduce into [])))

(s/defn change-secondaries-and-lookups
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (ddo [_ (change-lookups
           session
           entity
           old-record
           new-record
           opts)
        _ (change-secondaries
           session
           entity
           old-record
           new-record
           opts)]
    (return
     new-record)))

(s/defn cassandra-column-name?
  [k]
  (->> k
       name
       (re-matches #"\p{Alpha}[_\p{Alnum}]+")
       boolean))

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
   record :- t/RecordSchema
   opts :- fns/UpsertOptsSchema]

  (when (and (some? old-record)
             (not= (t/extract-uber-key-value entity old-record)
                   (t/extract-uber-key-value entity record)))
    (throw (pr/error-ex ::uber-key-mismatch
                        {:entity (-> entity :primary-table :name)
                         :old-record-uberkey (t/extract-uber-key-value entity old-record)
                         :record-uberkey (t/extract-uber-key-value entity record)
                         :old-record old-record
                         :record record
                         :opts opts})))
  (ddo [:let [opts (ts/default-timestamp-opt opts)

              ;; separate the tru cassandra columns from non-cassandra
              ;; columns which will be removed by callbacks
              {cassandra-cols true
               non-cassandra-cols false} (->> record
                                              keys
                                              (group-by cassandra-column-name?))
              non-cassandra-record (select-keys record non-cassandra-cols)

              old-record (not-empty old-record)]

        old-record-ser (when old-record
                         (cb/run-save-callbacks
                          session
                          entity
                          :serialize
                          old-record
                          old-record
                          opts))

        ;; :let [_ (warn "record" record)]

        record-ser (cb/chain-save-callbacks
                    session
                    entity
                    [:before-save :serialize]
                    old-record
                    record
                    opts)

        :let [;; don't need the ::t/skip-protect opt any more
              opts (dissoc opts ::t/skip-protect)

              record-keys (-> record keys set)
              record-ser-keys (-> record-ser keys set)
              removed-keys (set/difference record-keys record-ser-keys)

              ;; if the op is an insert, then old-record will be nil,
              ;; and we will need to return nil values for any removed keys
              ;; to preserve schema
              nil-removed (->> removed-keys
                               (filter cassandra-column-name?)
                               (map (fn [k]
                                      [k nil]))
                               (into {}))]

        [updated-record-with-keys-ser
         acquire-failures] (unique-key/upsert-primary-record-and-update-unique-keys
                            session
                            entity
                            old-record-ser
                            record-ser
                            opts)

        ;; :let [_ (warn "updated-record-with-keys-ser" updated-record-with-keys-ser)]

        _ (monad/when updated-record-with-keys-ser
            (change-secondaries-and-lookups session
                                            entity
                                            old-record-ser
                                            updated-record-with-keys-ser
                                            opts))

        ;; construct the response and deserialise
        response-record-raw (merge nil-removed
                                   old-record
                                   updated-record-with-keys-ser)

        ;; :let [_ (warn "response-record-raw" response-record-raw)]

        ;; since the :serialize phase can synthesize columns
        ;; we need to :deserialize to feed to the :after-save
        ;; callbacks, which may want to propagate the synthesized
        ;; columns
        old-record-deser (when old-record-ser
                           (cb/run-callbacks
                            session
                            entity
                            :deserialize
                            old-record-ser
                            opts))

        response-record-deser (cb/chain-callbacks
                               session
                               entity
                               [:deserialize]
                               response-record-raw
                               opts)

        ;; :let [_ (warn "response-record-deser" response-record-deser)]

        _ (cb/run-save-callbacks
           session
           entity
           :after-save
           old-record-deser
           response-record-deser
           opts)

        response-record (cb/chain-callbacks
                         session
                         entity
                         [:after-load]
                         response-record-deser
                         opts)

        ;; :let [_ (warn "response-record" response-record)]
        ]

    (return
     (pair response-record
           acquire-failures))))

(s/defn ^:deprecated upsert*
  "**DEPRECATED** use `select-upsert` or `change` instead

   upsert a single instance

   convenience fn - if the entity has any maintained foreign keys it borks"
  [session :- ModelSession
   entity :- Entity
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]
  (ddo [:let [has-maintained-foreign-keys?
              (not-empty
               (t/all-maintained-foreign-key-cols entity))]

        record-ser
        (cb/chain-save-callbacks
         session
         entity
         [:before-save :serialize]
         nil
         record
         opts)

        :let [collection-cols (->> record-ser
                                   (filter (comp coll? second))
                                   (map first)
                                   set)
              has-collection-cols? (seq collection-cols)]

        _ (monad/when has-collection-cols?
            (throw
             (pr/error-ex
              :upsert/unsafe-upsert-of-collection-column
              {:message (str "this entity has "
                             "un-serialized collection columns "
                             "for which upsert is not a safe operation. "
                             "either use select-upsert or change")
               :entitiy (with-out-str (pprint entity))
               :record record
               :collection-columns collection-cols
               :opts opts})))

        _ (monad/when has-maintained-foreign-keys?
            (throw
             (pr/error-ex
              :upsert/require-explicit-select-upsert
              {:message (str "this entity has foreign keys, "
                             "so requires the previous version "
                             "to upsert. either use select-upsert "
                             "or change")
               :entity (with-out-str (pprint entity))
               :record record
               :opts opts})))]

    (upsert-changes*
     session
     entity
     nil
     record
     opts)))

(s/defn select-upsert*
  "upsert a single instance

   convenience fn - if the entity has any maintained foreign keys it first
                    selects the instance from the db,
                    then calls upsert-changes*"
  [session :- ModelSession
   entity :- Entity
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsSchema]
  (ddo [:let [has-maintained-foreign-keys?
              (not-empty
               (t/all-maintained-foreign-key-cols entity))]

        ;; need to run :before-save on the record in case
        ;; it defaults something in the uberkey
        record-ser (cb/run-save-callbacks
                    session
                    entity
                    :before-save
                    record
                    record
                    opts)

        raw-old-record (r/select-one
                        session
                        (get-in entity [:primary-table :name])
                        (get-in entity [:primary-table :key])
                        (t/extract-uber-key-value
                         entity
                         record-ser))

        old-record (monad/when raw-old-record
                     (cb/chain-callbacks
                      session
                      entity
                      [:deserialize :after-load]
                      raw-old-record
                      opts))]

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
                                  (fns/upsert-opts->delete-opts opts))]
      (return [:delete old-record]))

    ;; don't :noop here - there may be callbacks which
    ;; synthesize a key. anti-tombstone protection will
    ;; catch true noops later
    ;; (= old-record record)
    ;; (return deferred-context [:noop record])

    :else
    (ddo [[ur acquire-failures] (upsert-changes*
                session
                entity
                old-record
                record
                opts)]
      (return [:upsert ur acquire-failures]))))
