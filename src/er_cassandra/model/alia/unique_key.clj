(ns er-cassandra.model.alia.unique-key
  (:require
   [cats
    [context :refer [with-context]]
    [core :refer [mlet return]]]
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
   [schema.core :as s])
  (:import
   er_cassandra.model.types.Entity
   er_cassandra.session.Session))

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
   (s/enum :key/inserted :key/owned :key/updated :key/notunique)])

(s/defn acquire-unique-key
  "acquire a single unique key.
   returns a Deferred[[:ok <keydesc> info]] if the key was acquired
   successfully, a ErrorDeferred[[:fail <keydesc> reason]]"

  [session :- Session
   entity :- Entity
   unique-key-table :- t/UniqueKeyTableSchema
   uber-key-value :- t/KeyValueSchema
   unique-key-record :- t/RecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema]

  (with-context deferred-context
      (mlet [:let [uber-key (t/uber-key entity)
                   key (:key unique-key-table)
                   key-value (k/extract-key-value key unique-key-record)
                   ;; can't use any PK components in an ONLY IF LWT
                   [lwt-update-key
                    lwt-update-key-value] (k/remove-key-components
                                           uber-key
                                           uber-key-value
                                           key)
                   key-desc {:uber-key uber-key :uber-key-value uber-key-value
                             :key key
                             :key-value key-value}]

             insert-response (r/insert session
                                       (:name unique-key-table)
                                       unique-key-record
                                       (merge (fns/opts-remove-timestamp opts)
                                              {:if-not-exists true}))

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
                                unique-key-record
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
                                unique-key-record
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
           )))))

(s/defschema ReleaseUniqueKeyResultSchema
  [(s/one StatusSchema :status)
   (s/one KeyDescSchema :key-desc)
   (s/enum :deleted :stale)])

(s/defn release-unique-key
  "remove a single unique key"
  [session :- Session
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

    (with-context deferred-context
      (mlet [delete-result (r/delete session
                                     (:name unique-key-table)
                                     key
                                     key-value
                                     delete-opts)
             deleted? (return (applied? delete-result))]
        (return
         (cond
           deleted? [:ok key-desc :deleted]
           :else    [:ok key-desc :stale]))))))

(s/defn release-stale-unique-keys-for-table
  [session :- Session
   entity :- Entity
   table :- t/UniqueKeyTableSchema
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [:let [uber-key (t/uber-key entity)
             uber-key-value (t/extract-uber-key-value entity old-record)]
       stale-kvs (l/stale-lookup-key-values-for-table
                  session
                  entity
                  old-record
                  new-record
                  table)
       release-responses (->> (for [kv stale-kvs]
                                (release-unique-key
                                 session
                                 entity
                                 table
                                 uber-key-value
                                 kv
                                 (fns/upsert-opts->using-only opts)))
                              combine-responses)]
      (return release-responses))))

(s/defn release-stale-unique-keys
  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [all-release-responses (->> (for [t (:unique-key-tables entity)]
                                    (release-stale-unique-keys-for-table
                                     session
                                     entity
                                     t
                                     old-record
                                     new-record
                                     opts))
                                  combine-responses)]
      (return
       (apply concat all-release-responses)))))

(s/defn acquire-unique-keys-for-table
  [session :- Session
   entity :- Entity
   table :- t/UniqueKeyTableSchema
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet
      [:let [uber-key (t/uber-key entity)
             uber-key-value (t/extract-uber-key-value entity record)]
       lookup-records (->> (l/generate-lookup-records-for-table
                            session entity table old-record record)
                           combine-responses)
       acquire-responses (->> (for [lr lookup-records]
                                (acquire-unique-key
                                 session
                                 entity
                                 table
                                 uber-key-value
                                 lr
                                 (fns/upsert-opts->using-only opts)))
                              combine-responses)]
      (return acquire-responses))))

(s/defn acquire-unique-keys
  [session :- Session
   entity :- Entity
   old-record :- t/MaybeRecordSchema
   record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [all-acquire-responses (->> (for [t (:unique-key-tables entity)]
                                        (acquire-unique-keys-for-table
                                         session
                                         entity
                                         t
                                         old-record
                                         record
                                         opts))
                                      combine-responses)]
      (return
       (apply concat all-acquire-responses)))))

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
  ([session :- Session
    entity :- Entity
    record :- t/MaybeRecordSchema
    {:keys [if-not-exists
            if-exists
            only-if
            using] :as opts} :- fns/UpsertOptsWithTimestampSchema]
   (with-context deferred-context
     (mlet [:let [primary-table-name (get-in entity [:primary-table :name])
                  primary-table-key (get-in entity [:primary-table :key])

                  nok-record (without-unique-keys entity record)

                  ;; prefer insert - because if a new record is updated
                  ;; into existence, all it's cols being nulled will cause
                  ;; its automatic deletion
                  insert? (or if-not-exists
                              (and (not if-exists) (not only-if)))]

            insert-response (cond
                              if-not-exists
                              (r/insert session
                                        primary-table-name
                                        nok-record
                                        (fns/opts-remove-timestamp opts))

                              (and (not if-exists) (not only-if))
                              (r/insert session
                                        primary-table-name
                                        nok-record
                                        opts)

                              :else ;; only-if
                              (return nil))

            :let [inserted? (cond
                              if-not-exists
                              (applied? insert-response)

                              (and (not if-exists) (not only-if)) true

                              :else false)]

            update-response (cond

                              (and (not insert?)
                                   (or if-exists only-if))
                              (r/update
                               session
                               primary-table-name
                               primary-table-key
                               nok-record
                               (fns/opts-remove-timestamp opts))

                              insert?
                              (return nil)

                              :else
                              (throw (ex-info
                                      "internal error"
                                      {:entity entity
                                       :record record
                                       :opts opts})))

            :let [updated? (applied? update-response)]

            ;; TODO - removing the reselect - it's
            ;; ok for an upsert to return just what was upserted...
            ;; but there are some upstream things to work through
            ;; to make it work properly

            upserted-record (cond
                              ;; definitely a first insert
                              (and inserted? if-not-exists)
                              (return nok-record)

                              ;; insert or update - must retrieve
                              (or inserted? updated?)
                              (r/select-one session
                                            primary-table-name
                                            primary-table-key
                                            (t/extract-uber-key-value
                                             entity nok-record))

                              ;; failure
                              :else
                              (return nil))
            ]

       (return
        (if (or inserted? updated?)
          [upserted-record
           nil]

          [nil [(e/general-error-log-entry
                 {:error-tag :upsert/primary-record-upsert-error
                  :message "couldn't upsert primary record"
                  :primary-table primary-table-name
                  :uber-key-value (t/extract-uber-key-value entity record)
                  :other {:record record
                          :if-not-exists if-not-exists
                          :only-if only-if}})]]))))))

(s/defn update-unique-keys-after-primary-upsert
  "attempts to acquire unique keys for an owner... returns
   a Deferred<[updated-owner-record failed-keys]> with an updated
   owner record containing only the keys that could be acquired"
  [session :- Session
   entity :- Entity
   old-key-record :- t/MaybeRecordSchema ;; record with old unique keys
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertUsingOnlyOptsWithTimestampSchema] ;; record with updated unique keys
  (with-context deferred-context
    (mlet [release-key-responses (release-stale-unique-keys
                                  session
                                  entity
                                  old-key-record
                                  new-record
                                  opts)
           acquire-key-responses (acquire-unique-keys
                                  session
                                  entity
                                  old-key-record
                                  new-record
                                  opts)
           acquire-failures (return
                             (describe-acquire-failures
                              entity
                              new-record
                              acquire-key-responses))
           updated-record (return
                           (update-record-by-key-responses
                            entity
                            old-key-record
                            new-record
                            acquire-key-responses))

           ;; only update the cols relating to the unique keys
           :let [all-uk-cols (all-unique-key-cols entity)
                 uberkey-cols (-> entity t/uber-key flatten set)

                 ;; we will set these if they are provided
                 set-cols (set/difference all-uk-cols uberkey-cols)

                 ;; only proceed if we have some col vals to set
                 set-vals (select-keys updated-record set-cols)

                 update-cols (into
                              all-uk-cols
                              uberkey-cols)]

           upsert-response (if-not (empty? set-vals)
                             (r/update
                              session
                              (get-in entity [:primary-table :name])
                              (get-in entity [:primary-table :key])
                              (select-keys updated-record
                                           update-cols)
                              opts)
                             (return nil))]
      (return [updated-record
               acquire-failures]))))

(s/defn upsert-primary-record-and-update-unique-keys
  "first upserts the primary record, with any constraints,
   then updates unique keys. returns a
   Deferred[updated-record-or-nil failure-descriptions]"
  [session :- Session
   entity :- Entity
   new-record :- t/MaybeRecordSchema
   opts :- fns/UpsertOptsWithTimestampSchema]
  (with-context deferred-context
    (mlet [[rec-old-keys
            upsert-errors] (upsert-primary-record-without-unique-keys
                            session
                            entity
                            new-record
                            opts)]
      (if rec-old-keys
        (update-unique-keys-after-primary-upsert session
                                                 entity
                                                 rec-old-keys
                                                 new-record
                                                 (fns/upsert-opts->using-only
                                                  opts))
        (return
         [nil upsert-errors])))))
