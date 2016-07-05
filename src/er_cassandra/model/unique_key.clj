(ns er-cassandra.model.unique-key
  (:require
   [clojure.set :as set]
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.error :as e]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]])
  (:import [er_cassandra.model.types Model]))

(defn applied?
  [lwt-response]
  (get lwt-response (keyword "[applied]")))

(defn applied-or-owned?
  [^Model model insert-uber-key lwt-insert-response ]
  (if (applied? lwt-insert-response)
    true
    (let [owner-uber-key (k/extract-key-value
                          (get-in model [:primary-table :key])
                          lwt-insert-response)]
      (= insert-uber-key owner-uber-key))))

(defn acquire-unique-key
  "acquire a single unique key.
   returns a Deferred[[:ok <keydesc> info]] if the key was acquired
   successfully, a ErrorDeferred[[:fail <keydesc> reason]]"

  [session ^Model model unique-key-table uber-key-value key-value]
  (let [uber-key (t/uber-key model)
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
                      model
                      (t/extract-uber-key-value model unique-key-record)
                      insert-response))

             live-ref? (if-not owned?
                         (r/select-one session
                                       (get-in model [:primary-table :name])
                                       (get-in model [:primary-table :key])
                                       (t/extract-uber-key-value
                                        model
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
                                        model
                                        insert-response)})
                                     (return nil))

             updated? (return (applied? stale-update-response))]

        (return
         (cond
           inserted? [:ok key-desc :inserted]    ;; new key
           owned?    [:ok key-desc :owned]       ;; ours already
           updated?  [:ok key-desc :updated]     ;; ours now
           live-ref? [:fail key-desc :notunique] ;; not ours
           :else     [:fail key-desc :notunique] ;; someone else won
           ))))))

(defn release-unique-key
  "remove a single unique key"
  [session ^Model model unique-key-table uber-key-value key-value]
  (let [uber-key (t/uber-key model)
        key (:key unique-key-table)
        ;; only-if can't reference primary-key components
        [npk-uber-key npk-uber-key-value] (k/remove-key-components
                                           uber-key
                                           uber-key-value
                                           key)
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}]
    (prn [npk-uber-key npk-uber-key-value])
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

(defn release-stale-unique-keys
  [session ^Model model old-record new-record]
  (combine-responses
   (mapcat
    identity
    (for [t (:unique-key-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model (or new-record
                                                               old-record))
            key (:key t)
            col-colls (:collections t)]
        (let [old-kvs (set (k/extract-key-value-collection key
                                                           old-record
                                                           col-colls))
              new-kvs (set (k/extract-key-value-collection key
                                                           new-record
                                                           col-colls))
              stale-kvs (filter identity (set/difference old-kvs new-kvs))]
          (for [kv stale-kvs]
            (release-unique-key session model t uber-key-value kv))))))))

(defn acquire-unique-keys
  [session ^Model model record]
  (combine-responses
   (mapcat
    identity
    (for [t (:unique-key-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model record)
            key (:key t)
            col-colls (:collections t)]
        (let [kvs (filter identity
                          (set (k/extract-key-value-collection key
                                                               record
                                                               col-colls)))]
          (for [kv kvs]
            (let [lookup-record (create-lookup-record
                                 uber-key uber-key-value
                                 key kv)]
              (acquire-unique-key session
                                  model
                                  t
                                  uber-key-value
                                  kv)))))))))

(defn update-with-acquire-responses
  "assume the the very last column in the key is the
   unique value that couldn't be acquired. perhaps this needs
   to become more flexible"
  [table acquire-key-responses record]
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

(defn describe-acquire-failures
  [^Model model requested-record acquire-key-responses]
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
         (e/field-error-log-entry
          field
          reason
          (str field " is not unique")
          field-value))))))

(defn responses-for-key
  [match-key responses]
  (filter (fn [[_ {:keys [key]} _]]
            (= key match-key))
          responses))

(defn update-record-by-key-responses
  [model old-record new-record acquire-key-responses]

  (reduce (fn [nr t]
            (let [ars (responses-for-key (:key t) acquire-key-responses)]
              (update-with-acquire-responses t ars nr)))
          new-record
          (:unique-key-tables model)))

(defn without-unique-keys
  "remove (the final part of) unique key columns from a record"
  [^Model model record]
  (let [unique-key-tabless (:unique-key-tables model)]
    (reduce (fn [r t]
              (let [key-col (last (:key t))]
                (dissoc r key-col)))
            record
            unique-key-tabless)))

(defn upsert-primary-record-without-unique-keys
  "attempts to upsert a primary record minus it's unique keys,
   possibly with an LWT and conditions if options if-not-exists or only-if
   are provided.
   returns a Deferred [upserted-record-or-nil failure-description]"
  ([session ^Model model record {:keys [if-not-exists only-if]}]
   (with-context deferred-context
     (mlet [primary-table-name (get-in model [:primary-table :name])
            primary-table-key (get-in model [:primary-table :key])

            ;; always test for presence if given an only-if condition
            :let [if-not-exists (boolean
                                 (or if-not-exists
                                     only-if))]

            nok-record (without-unique-keys model record)

            insert-response (if if-not-exists
                              (r/insert session
                                        primary-table-name
                                        nok-record
                                        {:if-not-exists true})
                              (r/insert session
                                        primary-table-name
                                        nok-record))

            :let [inserted? (if if-not-exists
                              (applied? insert-response)
                              true)]

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
                                             model nok-record))

                              ;; failure
                              :else
                              (return nil))]

       (return
        (if upserted-record
          [upserted-record nil]

          [nil [(e/general-error-log-entry
                 :upsert-primary-record
                 "couldn't upsert primary record"
                 {:record record
                  :if-not-exists if-not-exists
                  :only-if only-if})]]))))))

(defn update-unique-keys-after-primary-upsert
  "attempts to acquire unique keys for an owner... returns
   a Deferred[Right[[updated-owner-record failed-keys]]] with an updated
   owner record containing only the keys that could be acquired"
  [session
   ^Model model
   old-key-record ;; record with old unique keys
   new-record] ;; record with updated unique keys
  (with-context deferred-context
    (mlet [release-key-responses (release-stale-unique-keys
                                  session
                                  model
                                  old-key-record
                                  new-record)
           acquire-key-responses (acquire-unique-keys
                                  session
                                  model
                                  new-record)
           acquire-failures (return
                             (describe-acquire-failures
                              model
                              new-record
                              acquire-key-responses))
           updated-record (return
                           (update-record-by-key-responses
                            model
                            old-key-record
                            new-record
                            acquire-key-responses))
           upsert-response (r/insert
                            session
                            (get-in model [:primary-table :name])
                            updated-record)]
      (return [updated-record
               acquire-failures]))))

(defn upsert-primary-record-and-update-unique-keys
  "first upserts the primary record, with any constraints,
   then updates unique keys. returns a
   Deferred[updated-record-or-nil failure-descriptions]"
  ([session ^Model model new-record]
   (upsert-primary-record-and-update-unique-keys session model new-record {}))
  ([session ^Model model new-record opts]
   (with-context deferred-context
     (mlet [[rec-old-keys
             upsert-errors] (upsert-primary-record-without-unique-keys
                             session
                             model
                             new-record
                             opts)]
       (if rec-old-keys
         (update-unique-keys-after-primary-upsert session
                                                  model
                                                  rec-old-keys
                                                  new-record)
         (return
          [nil upsert-errors]))))))
