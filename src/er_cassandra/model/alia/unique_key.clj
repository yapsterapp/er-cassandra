(ns er-cassandra.model.alia.unique-key
  (:require
   [cats
    [context :refer [with-context]]
    [core :as monad :refer [mlet return]]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.set :as set]
   [er-cassandra
    [key :as k]
    [record :as r]]
   [er-cassandra.model
    [error :as e]
    [types :as t]
    [util :refer [combine-responses create-lookup-record]]]
   [er-cassandra.model.alia
    [fn-schema :as fns]
    [lookup :as l]]
   [manifold.stream :as stream]
   [prpr.promise :as pr :refer [ddo]]
   [schema.core :as s]
   [taoensso.timbre :refer [info warn]]
   [er-cassandra.model.alia.minimal-change :as min.ch]
   [er-cassandra.model.alia.lookup :as lookup]
   [er-cassandra.model.types :as t ])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn applied?
  [lwt-response]
  (get lwt-response (keyword "[applied]")))

(defn applied-or-owned?
  [^Entity entity insert-uber-key lwt-insert-response ]
  (if (applied? lwt-insert-response)
    true
    (let [owner-uber-key (k/extract-key-value
                          (get-in entity [:primary-table :key])
                          lwt-insert-response)]
      (= insert-uber-key owner-uber-key))))

(s/defschema StatusSchema (s/enum :ok :fail))

(s/defschema KeyDescSchema
  {:uber-key t/PrimaryKeySchema
   :uber-key-value t/KeyValueSchema
   :key t/PrimaryKeySchema
   :key-value t/KeyValueSchema})

(s/defschema AcquireUniqueKeyResultSchema
  [(s/one StatusSchema :status)
   (s/one KeyDescSchema :key-desc)
   (s/enum :key/inserted :key/owned :key/updated :key/notunique :key/nochange)])

(s/defn acquire-unique-key
  "acquire a single unique key with an LWT. if the key doesn't exist
   or is a dead reference it will be updated instead

   returns a Deferred[[:ok <keydesc> info]] if the key was acquired
   successfully, a ErrorDeferred[[:fail <keydesc> reason]]"

  [session :- ModelSession
   entity :- Entity
   unique-key-table :- t/UniqueKeyTableSchema
   uber-key-value :- t/KeyValueSchema
   unique-key-record :- t/RecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]

  (ddo [:let [uber-key (t/uber-key entity)
              uq-key (:key unique-key-table)
              key-value (k/extract-key-value uq-key unique-key-record)
              ;; can't use any PK components in an ONLY IF LWT
              [lwt-update-key
               lwt-update-key-value] (k/remove-key-components
                                      uber-key
                                      uber-key-value
                                      uq-key)
              key-desc {:uber-key uber-key
                        :uber-key-value uber-key-value
                        :key uq-key
                        :key-value key-value}

              ;; never nil, since old-unique-key-record is nil
              min-change (min.ch/avoid-tombstone-change-for-table
                          unique-key-table
                          nil ;; always nil for acquire
                          unique-key-record)

              has-collection-cols? (some
                                    (comp t/is-collection-column-diff? second)
                                    min-change)]

        insert-response (if has-collection-cols?
                          (r/update session
                                    (:name unique-key-table)
                                    (:key unique-key-table)
                                    min-change
                                    (merge (fns/opts-remove-timestamp opts)
                                           {:only-if
                                            (k/extract-key-equality-clause
                                             lwt-update-key
                                             lwt-update-key-value)}))
                          (r/insert session
                                    (:name unique-key-table)
                                    min-change
                                    (merge (fns/opts-remove-timestamp opts)
                                           {:if-not-exists true})))

        :let [inserted? (applied? insert-response)
              owned? (applied-or-owned?
                      entity
                      (t/extract-uber-key-value entity unique-key-record)
                      insert-response)]

        live-ref? (if-not owned?
                    (r/select-one session
                                  (get-in entity [:primary-table :name])
                                  (get-in entity [:primary-table :key])
                                  (t/extract-uber-key-value
                                   entity
                                   insert-response))
                    (return nil))

        ;; TODO - check that primary record has a live forward reference,
        ;;        or lookup is really stale despite primary existing

        update-response (cond
                          (and (not inserted?) owned?)
                          (r/update
                           session
                           (:name unique-key-table)
                           (:key unique-key-table)
                           min-change
                           (merge
                            (fns/opts-remove-timestamp opts)
                            {:only-if
                             (k/extract-key-equality-clause
                              lwt-update-key
                              lwt-update-key-value)}))

                          (and (not inserted?) (not owned?) (not live-ref?))
                          (r/update
                           session
                           (:name unique-key-table)
                           (:key unique-key-table)
                           min-change
                           (merge
                            (fns/opts-remove-timestamp opts)
                            {:only-if
                             (k/extract-key-equality-clause
                              lwt-update-key
                              insert-response)}))

                          :else
                          (return nil))

        updated? (return (applied? update-response))]

    (return
     (cond
       inserted? [:ok key-desc :key/inserted]    ;; new key
       owned?    [:ok key-desc :key/owned]       ;; ours already
       updated?  [:ok key-desc :key/updated]     ;; ours now
       live-ref? [:fail key-desc :key/notunique] ;; not ours
       :else     [:fail key-desc :key/notunique] ;; someone else won
       ))))

(s/defn update-unique-key
  "update a single unique key with an LWT to ensure that it belongs
   to the right entity.

   TODO if the update fails because the key doesn't exist, then
   insert it instead

   returns a Deferred[[:ok <keydesc> info]] if the key was acquired
   successfully, a ErrorDeferred[[:fail <keydesc> reason]]

   NOTE this op will return an :ok status if a record mistakenly
   thinks it already has the unique key - this seems like a good
   tradeoff for avoiding LWTs on every update of a record with a
   unique key"

  [session :- ModelSession
   entity :- Entity
   unique-key-table :- t/UniqueKeyTableSchema
   uber-key-value :- t/KeyValueSchema
   old-unique-key-record :- t/RecordSchema
   unique-key-record :- t/RecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]

  (ddo [:let [uber-key (t/uber-key entity)
              uq-key (:key unique-key-table)
              key-value (k/extract-key-value uq-key unique-key-record)

              ;; can't use any PK components in an ONLY IF LWT
              [lwt-update-key
               lwt-update-key-value] (k/remove-key-components
                                      uber-key
                                      uber-key-value
                                      uq-key)

              key-desc {:uber-key uber-key
                        :uber-key-value uber-key-value
                        :key uq-key
                        :key-value key-value}

              ;; we're using minimal-change-for-table here because
              ;; unique-keys are "safer" than other keys, because they
              ;; aren't added to the primary-table record until after
              ;; they have been acquired, so the risk of inconsistency is
              ;; small.
              min-change (min.ch/minimal-change-for-table
                          unique-key-table
                          old-unique-key-record
                          unique-key-record)

              ;; _ (warn "update-unique-key"
              ;;         {:min-change min-change
              ;;          :lwt-update-key lwt-update-key
              ;;          :lwt-update-key-value lwt-update-key-value})
              ]

        update-response (monad/when min-change
                          (r/update
                           session
                           (:name unique-key-table)
                           (:key unique-key-table)
                           min-change
                           (merge
                            (fns/opts-remove-timestamp opts)
                            {:only-if
                             (k/extract-key-equality-clause
                              lwt-update-key
                              lwt-update-key-value)})))

        :let [updated? (when update-response
                         (applied? update-response))]]

    (return
     (cond
       (nil? min-change) [:ok key-desc :key/nochange]
       updated?  [:ok key-desc :key/updated]
       :else     [:fail key-desc :key/notunique]))))

(s/defschema ReleaseUniqueKeyResultSchema
  [(s/one StatusSchema :status)
   (s/one KeyDescSchema :key-desc)
   (s/enum :deleted :stale)])

(s/defn release-unique-key
  "remove a single unique key"
  [session :- ModelSession
   entity :- Entity
   unique-key-table :- t/UniqueKeyTableSchema
   uber-key-value :- t/KeyValueSchema
   key-value :- t/KeyValueSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]

  (let [uber-key (t/uber-key entity)
        key (:key unique-key-table)
        ;; only-if can't reference primary-key components
        [npk-uber-key npk-uber-key-value] (k/remove-key-components
                                           uber-key
                                           uber-key-value
                                           key)
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}

        delete-opts (merge
                     (-> opts
                         fns/upsert-opts->delete-opts
                         fns/opts-remove-timestamp)
                     (when (not-empty npk-uber-key)
                       {:only-if (k/key-equality-clause
                                  npk-uber-key
                                  npk-uber-key-value)}))]

    (ddo [delete-result (r/delete session
                                  (:name unique-key-table)
                                  key
                                  key-value
                                  delete-opts)
          deleted? (return (applied? delete-result))]
      (return
       (cond
         deleted? [:ok key-desc :deleted]
         :else    [:ok key-desc :stale])))))

(s/defn update-with-acquire-responses
  "remove unique key values which couldn't be acquired
   from the entity record

   assumes the the very last column in the key is the
   unique value that couldn't be acquired. perhaps this needs
   to become more flexible"
  [table :- t/UniqueKeyTableSchema
   acquire-key-responses :- [AcquireUniqueKeyResultSchema]
   record  :- t/MaybeRecordSchema]
  (reduce (fn [r [status
                  {:keys [uber-key uber-key-value
                          key key-value]:as key-desc}
                  _]]
            (let [col-colls (:collections table)
                  key-col (last key)
                  coll (get col-colls key-col)
                  key-val (last key-value)]
              (if (= :ok status)
                r
                (condp = coll

                  :set (assoc r
                              key-col
                              (disj (get record key-col)
                                    key-val))
                  :list (assoc r
                               key-col
                               (filterv #(not= key-val %)
                                        (get record key-col)))

                  :map (assoc r
                              key-col
                              (dissoc (get record key-col)
                                      key-val))

                  (assoc r key-col nil)))))
          record
          acquire-key-responses))

(s/defn describe-acquire-failures
  [entity :- Entity
   requested-record :- t/MaybeRecordSchema
   acquire-key-responses :- [AcquireUniqueKeyResultSchema]]
  (let [failures (filter (fn [[status key-desc reason]]
                           (not= :ok status))
                         acquire-key-responses)
        by-key (group-by (fn [[status {:keys [key key-value]} reason]]
                           key)
                         failures)]
    (into
     []
     (for [[status
            {:keys [key key-value] :as key-desc}
            reason] failures]
       (let [field (last key)
             field-value (last key-value)]
         (e/key-error-log-entry
          {:error-tag reason
           :message (str field " is not unique: " field-value)
           :primary-table (-> entity :primary-table :name)
           :uber-key-value (t/extract-uber-key-value entity requested-record)
           :key key
           :key-value key-value}))))))

(s/defn responses-for-key
  [match-key :- t/PrimaryKeySchema
   responses :- [AcquireUniqueKeyResultSchema]]
  (filter (fn [[_ {:keys [key]} _]]
            (= key match-key))
          responses))

(s/defn update-record-by-key-responses
  [entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   acquire-key-responses :- [AcquireUniqueKeyResultSchema]]

  (reduce (fn [nr t]
            (let [ars (responses-for-key (:key t) acquire-key-responses)]
              (update-with-acquire-responses t ars nr)))
          new-record
          (:unique-key-tables entity)))

(s/defn all-unique-key-cols
  "return a set of the union of all unique-key columns in the entity,
   which will be used to update only the unique-key columsn after key
   acquisition"
  [entity :- Entity]
  (let [unique-key-tables (:unique-key-tables entity)]
    (reduce (fn [cols t]
              (into cols (flatten (:key t))))
            #{}
            unique-key-tables)))

(s/defn without-unique-keys
  "remove (the final part of) unique key columns from a record"
  [entity :- Entity
   record :- t/MaybeRecordSchema]
  (let [unique-key-tables (:unique-key-tables entity)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (dissoc r key-col)))
            record
            unique-key-tables)))

(s/defn upsert-primary-record-without-unique-keys
  "attempts to upsert a primary record minus it's unique keys,
   possibly with an LWT and conditions if options if-not-exists or only-if
   are provided.
   returns a Deferred [upserted-record-or-nil failure-description]"
  ([session :- ModelSession
    entity :- Entity
    old-record :- t/MaybeRecordSchema
    record :- t/MaybeRecordSchema
    {:keys [if-not-exists
            if-exists
            only-if
            using] :as opts} :- fns/UpsertOptsWithTimestampSchema]
   (let [nok-record (without-unique-keys entity record)
         nok-old-record (select-keys
                         (without-unique-keys entity old-record)
                         (keys nok-record))

         ;; we will actually upsert the minimum change, with
         ;; any unchanged cols removed... this is safe for
         ;; the primary table, less safe for the (non-unique)
         ;; index tables (so we use avoid-tombstone-change-for-table
         ;; for those tables)
         min-change (min.ch/minimal-change-for-table
                     (:primary-table entity)
                     nok-old-record
                     nok-record)

         has-collection-cols? (some
                               (comp t/is-collection-column-diff? second)
                               min-change)

         ;; _ (warn "upsert-primary-record-without-unique-keys"
         ;;         {:primary-table (:primary-table entity)
         ;;          :old-record old-record
         ;;          :record record
         ;;          :nok-old-record nok-old-record
         ;;          :nok-record nok-record
         ;;          :min-change min-change})
         ]
     (if (nil? min-change)
       (return deferred-context nok-record)
       (ddo [:let [primary-table-name (get-in entity [:primary-table :name])
                   primary-table-key (get-in entity [:primary-table :key])

                   ;; prefer insert - because if a new record is updated
                   ;; into existence, all it's cols being nulled will cause
                   ;; its automatic deletion
                   ;;
                   ;; for records containing collection columns our tombstone
                   ;; avoidance will protect against the null columns causing
                   ;; deletion case
                   ;;
                   ;; records containing collection columns can be safely
                   ;; INSERTed if the collection column isn't present (as
                   ;; INSERTs won't trigger the automatic deletion)
                   ;;
                   ;; if a collection column is present in the record then
                   ;; UPDATE will *always* be used _and_ the collection column
                   ;; will *never* be nil'led (by virtue of the column only ever
                   ;; being SET using the collection altering + and - operators)
                   insert? (and
                            (not has-collection-cols?)
                            (or if-not-exists
                                (and (not if-exists) (not only-if))))]

             insert-response (cond
                               (not insert?)
                               (return nil)

                               if-not-exists
                               (r/insert session
                                         primary-table-name
                                         min-change
                                         (fns/opts-remove-timestamp opts))

                               (and (not if-exists) (not only-if))
                               (r/insert session
                                         primary-table-name
                                         min-change
                                         opts))

             :let [inserted? (cond
                               (not insert?)
                               false

                               if-not-exists
                               (applied? insert-response)

                               (and (not if-exists) (not only-if))
                               true)]

             update-response (cond
                               insert?
                               (return nil)

                               (or if-exists only-if)
                               (r/update
                                session
                                primary-table-name
                                primary-table-key
                                min-change
                                (fns/opts-remove-timestamp opts))

                               has-collection-cols?
                               (r/update
                                session
                                primary-table-name
                                primary-table-key
                                min-change
                                opts)

                               :else
                               (throw (ex-info
                                       "internal error"
                                       {:entity entity
                                        :record record
                                        :opts opts})))

             :let [updated? (or has-collection-cols?
                                (applied? update-response))]

             upserted-record (cond
                               ;; return what was upserted or nil
                               (or inserted? updated?)
                               (return nok-record)

                               ;; failure
                               :else
                               (return nil))]

         (return
          (if (or inserted? updated?)
            upserted-record

            (throw
             (pr/error-ex
              :upsert/primary-record-upsert-error
              {:error-tag :upsert/primary-record-upsert-error
               :message "couldn't upsert primary record"
               :primary-table primary-table-name
               :uber-key-value (t/extract-uber-key-value entity record)
               :record record
               :if-not-exists if-not-exists
               :only-if only-if})))))))))

(s/defn change-unique-key
  "change a single unique-key - will release, acquire or update
   as appropriate, and will do a minimal update (avoiding
   tombstone and garbage creation)"
  [session :- ModelSession
   entity :- Entity
   {t-key :key
    :as table} :- t/UniqueKeyTableSchema
   old-record :- t/MaybeRecordSchema ;; record with old unique keys
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema
   [old-unique-key-record
    new-unique-key-record
    :as uk-change] :- t/ChangeSchema]
  (cond
    (and (nil? old-unique-key-record)
         (nil? new-unique-key-record))
    (throw
     (pr/error-ex ::unique-key-change-both-nil
                  {:entity entity
                   :table table
                   :old-record old-record
                   :new-record new-record
                   :change uk-change}))

    (nil? new-unique-key-record)
    (ddo [:let [ukv (t/extract-uber-key-value entity (or new-record old-record))
                kv (k/extract-key-value t-key old-unique-key-record)]
          rukr (release-unique-key
                session
                entity
                table
                ukv
                kv
                opts)]
      (return
       {:release-key-responses [rukr]}))

    (nil? old-unique-key-record)
    (ddo [:let [ukv (t/extract-uber-key-value entity (or new-record old-record))]
          aukr (acquire-unique-key
                session
                entity
                table
                ukv
                new-unique-key-record
                opts)]
      (return
       {:acquire-key-responses [aukr]}))

    :else ;; update or noop
    (ddo [:let [ukv (t/extract-uber-key-value entity (or new-record old-record))]
          uukr (update-unique-key
                session
                entity
                table
                ukv
                old-unique-key-record
                new-unique-key-record
                opts)]
      (return
       {:acquire-key-responses [uukr]}))))

(s/defn change-unique-keys-for-table
  [session :- ModelSession
   entity :- Entity
   table :- t/UniqueKeyTableSchema
   old-record :- t/MaybeRecordSchema ;; record with old unique keys
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (ddo [uk-changes (lookup/generate-lookup-changes-for-table
                    session
                    entity
                    table
                    old-record
                    new-record)]
    (if uk-changes
      (->> uk-changes
           (stream/->source)
           (stream/map (partial
                        change-unique-key
                        session
                        entity
                        table
                        old-record
                        new-record
                        opts))
           (stream/realize-each)
           (stream/reduce
            (fn [a b]
              (merge-with (fnil into []) a b))
            {}))
      (return {}))))

(s/defn change-unique-keys
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema ;; record with old unique keys
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]
  (->> (:unique-key-tables entity)
       (stream/->source)
       (stream/map #(change-unique-keys-for-table
                     session
                     entity
                     %
                     old-record
                     new-record
                     opts))
       (stream/realize-each)
       (stream/reduce
        (fn [a b]
          (merge-with (fnil into []) a b))
        {})))

(s/defn update-unique-keys-after-primary-upsert
  "attempts to acquire unique keys for an owner... returns
   a Deferred<[updated-owner-record failed-keys]> with an updated
   owner record containing only the keys that could be acquired"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema ;; record with old unique keys
   new-record :- t/MaybeRecordSchema
   {minimal-change? ::t/minimal-change
    :as opts} :- fns/UpsertUsingOnlyOptsWithTimestampSchema] ;; record with updated unique keys

  (ddo [{release-key-responses :release-key-responses
         acquire-key-responses :acquire-key-responses}
        (change-unique-keys
         session
         entity
         old-record
         new-record
         opts)

        :let [acquire-failures (describe-acquire-failures
                                entity
                                new-record
                                acquire-key-responses)

              updated-record (update-record-by-key-responses
                              entity
                              old-record
                              new-record
                              acquire-key-responses)

              ;; only update the cols relating to the unique keys
              all-uk-cols (all-unique-key-cols entity)
              uberkey-cols (-> entity t/uber-key flatten set)
              all-uk-and-uberkey-cols (into uberkey-cols all-uk-cols)

              ;; one of these has to be in the change or it's a no-op
              uk-change-cols (set/difference all-uk-cols uberkey-cols)

              ;; the minimum change considering only unique-key cols
              ;; (and uberkey cols obvs)
              min-ch ((if minimal-change?
                        min.ch/minimal-change-for-table
                        min.ch/avoid-tombstone-change-for-table)
                      (:primary-table entity)
                      (select-keys old-record all-uk-and-uberkey-cols)
                      (select-keys updated-record all-uk-and-uberkey-cols))

              ;; _ (warn "update-unique-keys-after-primary-upsert"
              ;;         opts
              ;;         min-ch)
              ]

        ;; only update when we have some change outside of the uberkey cols
        ;; (otherwise deletes can leave a naked uberkey min-ch which
        ;;  will bork an update statement)
        upsert-response (monad/when (not-empty
                                     (select-keys min-ch uk-change-cols))
                          (r/update
                           session
                           (get-in entity [:primary-table :name])
                           (get-in entity [:primary-table :key])
                           min-ch
                           opts))]
    (return [updated-record
             acquire-failures])))

(s/defn upsert-primary-record-and-update-unique-keys
  "first upserts the primary record, with any constraints,
   then updates unique keys. returns a
   Deferred[updated-record-or-nil failure-descriptions]"
  [session :- ModelSession
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]

  (assert (or (nil? old-record)
              (nil? record)
              (= (t/extract-uber-key-value entity old-record)
                 (t/extract-uber-key-value entity record))))

  (ddo [_ (upsert-primary-record-without-unique-keys
                         session
                         entity
                         old-record
                         record
                         opts)]
    (update-unique-keys-after-primary-upsert
     session
     entity
     old-record
     record
     (fns/primary-upsert-opts->lookup-upsert-opts opts))))
