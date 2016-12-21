(ns er-cassandra.model.alia.unique-key
  (:require
   [clojure.set :as set]
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [schema.core :as s]
   [er-cassandra.session]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.error :as e]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.session Session]))

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

(s/defschema MaybeRecordSchema (s/maybe {s/Keyword s/Any}))

(s/defschema KeyDescSchema
  {:uber-key t/KeySchema
   :uber-key-value t/KeyValueSchema
   :key t/KeySchema
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
   key-value :- t/KeyValueSchema]

  (let [uber-key (t/uber-key entity)
        key (:key unique-key-table)
        unique-key-record (create-lookup-record
                           uber-key uber-key-value
                           key key-value)
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}]

    (with-context deferred-context
      (mlet [insert-response (r/insert session
                                       (:name unique-key-table)
                                       unique-key-record
                                       {:if-not-exists true})

             inserted? (return (applied? insert-response))

             owned? (return
                     (applied-or-owned?
                      entity
                      (t/extract-uber-key-value entity unique-key-record)
                      insert-response))

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

             stale-update-response (if (and (not owned?)
                                            (not live-ref?))
                                     (r/update
                                      session
                                      (:name unique-key-table)
                                      (:key unique-key-table)
                                      unique-key-record
                                      {:only-if
                                       (t/extract-uber-key-equality-clause
                                        entity
                                        insert-response)})
                                     (return nil))

             updated? (return (applied? stale-update-response))]

        (return
         (cond
           inserted? [:ok key-desc :key/inserted]    ;; new key
           owned?    [:ok key-desc :key/owned]       ;; ours already
           updated?  [:ok key-desc :key/updated]     ;; ours now
           live-ref? [:fail key-desc :key/notunique] ;; not ours
           :else     [:fail key-desc :key/notunique] ;; someone else won
           ))))))

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
   key-value :- t/KeyValueSchema]

  (let [uber-key (t/uber-key entity)
        key (:key unique-key-table)
        ;; only-if can't reference primary-key components
        [npk-uber-key npk-uber-key-value] (k/remove-key-components
                                           uber-key
                                           uber-key-value
                                           key)
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}]
    (with-context deferred-context
      (mlet [delete-result (r/delete session
                                     (:name unique-key-table)
                                     key
                                     key-value
                                     (when (not-empty npk-uber-key)
                                       {:only-if
                                        (k/key-equality-clause
                                         npk-uber-key
                                         npk-uber-key-value)}))
             deleted? (return (applied? delete-result))]
        (return
         (cond
           deleted? [:ok key-desc :deleted]
           :else    [:ok key-desc :stale]))))))

(s/defn stale-unique-key-values :- [t/KeyValueSchema]
  [entity :- Entity
   old-record :- MaybeRecordSchema
   new-record :- MaybeRecordSchema
   unique-key-table :- t/UniqueKeyTableSchema]
  (let [key (:key unique-key-table)
        col-colls (:collections unique-key-table)]
    (when (k/has-key? key new-record)
      (let [old-kvs (set (k/extract-key-value-collection key
                                                         old-record
                                                         col-colls))
            new-kvs (set (k/extract-key-value-collection key
                                                         new-record
                                                         col-colls))]
        (filter identity (set/difference old-kvs new-kvs))))))

(s/defn release-stale-unique-keys
  [session :- Session
   entity :- Entity
   old-record :- MaybeRecordSchema
   new-record :- MaybeRecordSchema]
  (combine-responses
   (mapcat
    identity
    (for [t (:unique-key-tables entity)]
      (let [uber-key (t/uber-key entity)
            uber-key-value (t/extract-uber-key-value entity old-record)
            stale-kvs (stale-unique-key-values entity old-record new-record t)]
        (for [kv stale-kvs]
          (release-unique-key session entity t uber-key-value kv)))))))

(s/defn acquire-unique-keys
  [session :- Session
   entity :- Entity
   record :- MaybeRecordSchema]
  (combine-responses
   (mapcat
    identity
    (for [t (:unique-key-tables entity)]
      (let [uber-key (t/uber-key entity)
            uber-key-value (t/extract-uber-key-value entity record)
            key (:key t)
            col-colls (:collections t)]
        (when (k/has-key? key record)
          (let [kvs (filter identity
                            (set (k/extract-key-value-collection key
                                                                 record
                                                                 col-colls)))]
            (for [kv kvs]
              (let [lookup-record (create-lookup-record
                                   uber-key uber-key-value
                                   key kv)]
                (acquire-unique-key session
                                    entity
                                    t
                                    uber-key-value
                                    kv))))))))))

(s/defn update-with-acquire-responses
  "remove unique key values which couldn't be acquired
   from the entity record

   assumes the the very last column in the key is the
   unique value that couldn't be acquired. perhaps this needs
   to become more flexible"
  [table :- t/UniqueKeyTableSchema
   acquire-key-responses :- [AcquireUniqueKeyResultSchema]
   record  :- MaybeRecordSchema]
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
   requested-record :- MaybeRecordSchema
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
  [match-key :- t/KeySchema
   responses :- [AcquireUniqueKeyResultSchema]]
  (filter (fn [[_ {:keys [key]} _]]
            (= key match-key))
          responses))

(s/defn update-record-by-key-responses
  [entity :- Entity
   old-record :- MaybeRecordSchema
   new-record :- MaybeRecordSchema
   acquire-key-responses :- [AcquireUniqueKeyResultSchema]]

  (reduce (fn [nr t]
            (let [ars (responses-for-key (:key t) acquire-key-responses)]
              (update-with-acquire-responses t ars nr)))
          new-record
          (:unique-key-tables entity)))

(s/defn without-unique-keys
  "remove (the final part of) unique key columns from a record"
  [entity :- Entity
   record :- MaybeRecordSchema]
  (let [unique-key-tabless (:unique-key-tables entity)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (dissoc r key-col)))
            record
            unique-key-tabless)))

(s/defn upsert-primary-record-without-unique-keys
  "attempts to upsert a primary record minus it's unique keys,
   possibly with an LWT and conditions if options if-not-exists or only-if
   are provided.
   returns a Deferred [upserted-record-or-nil failure-description]"
  ([session :- Session
    entity :- Entity
    record :- MaybeRecordSchema
    {:keys [if-not-exists
            only-if]} :- {(s/optional-key :if-not-exists) (s/maybe s/Bool)
                          (s/optional-key :only-if) (s/maybe [s/Any])}]
   (assert (not (and if-not-exists only-if)))
   (with-context deferred-context
     (mlet [primary-table-name (get-in entity [:primary-table :name])
            primary-table-key (get-in entity [:primary-table :key])

            nok-record (without-unique-keys entity record)

            insert-response (cond
                              if-not-exists
                              (r/insert session
                                        primary-table-name
                                        nok-record
                                        {:if-not-exists true})

                              (not only-if)
                              (r/insert session
                                        primary-table-name
                                        nok-record)

                              :else ;; only-if
                              (return nil))

            :let [inserted? (cond
                              if-not-exists
                              (applied? insert-response)

                              (not only-if) true

                              :else false)]

            update-response (if (and (not inserted?)
                                     only-if)
                              (r/update
                               session
                               primary-table-name
                               primary-table-key
                               nok-record
                               {:only-if only-if})
                              (return nil))

            :let [updated? (applied? update-response)]

            upserted-record (cond
                              ;; definitely a first insert
                              (and inserted? if-not-exists)
                              nok-record

                              ;; insert or update - must retrieve
                              (or inserted? updated?)
                              (r/select-one session
                                            primary-table-name
                                            primary-table-key
                                            (t/extract-uber-key-value
                                             entity nok-record))

                              ;; failure
                              :else
                              (return nil))]

       (return
        (if upserted-record
          [upserted-record nil]

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
   old-key-record :- MaybeRecordSchema ;; record with old unique keys
   new-record :- MaybeRecordSchema] ;; record with updated unique keys
  (with-context deferred-context
    (mlet [release-key-responses (release-stale-unique-keys
                                  session
                                  entity
                                  old-key-record
                                  new-record)
           acquire-key-responses (acquire-unique-keys
                                  session
                                  entity
                                  new-record)
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
           upsert-response (r/insert
                            session
                            (get-in entity [:primary-table :name])
                            updated-record)]
      (return [updated-record
               acquire-failures]))))

(s/defn upsert-primary-record-and-update-unique-keys
  "first upserts the primary record, with any constraints,
   then updates unique keys. returns a
   Deferred[updated-record-or-nil failure-descriptions]"
  ([session :- Session
    entity :- Entity
    new-record :- MaybeRecordSchema]
   (upsert-primary-record-and-update-unique-keys session entity new-record {}))
  ([session :- Session
    entity :- Entity
    new-record :- MaybeRecordSchema
    opts]
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
                                                  new-record)
         (return
          [nil upsert-errors]))))))
