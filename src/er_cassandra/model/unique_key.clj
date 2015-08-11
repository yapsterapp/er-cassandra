(ns er-cassandra.model.unique-key
  (:require
   [clojure.set :as set]
   [clojure.core.match :refer [match]]
   [manifold.deferred :as d]
   [cats.core :as m]
   [cats.monad.either :as either]
   [cats.monad.deferred :as dm]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.key :as k]
   [er-cassandra.record-either :as r]
   [er-cassandra.model.types :as t]
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
   returns a Deferred[Right[:ok <keydesc> info]] if the key was acquired
   successfully, a Deferred[Right[:fail <keydesc> reason]]"

  [session ^Model model unique-key-table uber-key-value key-value]
  (let [uber-key (t/uber-key model)
        key (:key unique-key-table)
        unique-key-record (create-lookup-record
                           uber-key uber-key-value
                           key key-value)
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}]

    (m/with-monad dm/either-deferred-monad
      (m/mlet [insert-response (r/insert session
                                          (:name unique-key-table)
                                          unique-key-record
                                          {:if-not-exists true})

               inserted? (m/return (applied? insert-response))

               owned? (m/return
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
                           (m/return nil))

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
                                       (m/return nil))

               updated? (m/return (applied? stale-update-response))]

              (m/return
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
        key-desc {:uber-key uber-key :uber-key-value uber-key-value
                  :key key :key-value key-value}]
    (m/with-monad dm/either-deferred-monad
      (m/mlet [delete-result (r/delete session
                                        (:name unique-key-table)
                                        key
                                        key-value
                                        {:only-if
                                         (k/key-equality-clause
                                          uber-key
                                          uber-key-value)})
               deleted? (m/return (applied? delete-result))]
              (m/return
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
            coll (:collection t)]
        (if coll
          (let [old-kvs (set (k/extract-key-value-collection key old-record))
                new-kvs (set (k/extract-key-value-collection key new-record))
                stale-kvs (filter identity (set/difference new-kvs old-kvs))]
            (for [kv stale-kvs]
              (release-unique-key session model t uber-key-value kv)))

          (let [old-kv (k/extract-key-value key old-record)
                new-kv (k/extract-key-value key new-record)]
            (when (and old-kv
                       (not= old-kv new-kv))
              [(release-unique-key session model t uber-key-value old-kv)]))))))))

(defn acquire-unique-keys
  [session ^Model model record]
  (combine-responses
   (mapcat
    identity
    (for [t (:lookup-tables model)]
      (let [uber-key (t/uber-key model)
            uber-key-value (t/extract-uber-key-value model record)
            key (:key t)
            coll (:collection t)]
        (if coll
          (let [kvs (filter identity
                            (set (k/extract-key-value-collection key record)))]
            (for [kv kvs]
              (let [lookup-record (create-lookup-record
                                   uber-key uber-key-value
                                   key kv)]
                (acquire-unique-key session
                                    model
                                    t
                                    uber-key-value
                                    kv))))

          (when-let [key-value (k/extract-key-value key record)]
            [(acquire-unique-key session
                                 model
                                 t
                                 uber-key-value
                                 key-value)])))))))

(defn update-with-acquire-responses
  [table acquire-key-responses record]
  (reduce (fn [[status key-desc _]]
            (let [coll (:collection table)
                  key-col (last (:key key-desc))
                  key-val (last (:key-value key-desc))]
              (condp = coll
                :set (when-not (= :ok status)
                       (assoc record
                              key-col
                              (disj (get record key-col)
                                    key-val)))
                :list (when-not (= :ok status)
                        (assoc record
                               key-col
                               (filterv #(not= key-val %)
                                        (get record key-col))))

                :map (when-not (= :ok status)
                       (assoc record
                              key-col
                              (dissoc (get record key-col)
                                      key-val)))

                :else (when-not (= :ok status)
                        (assoc record key-col nil)))))
          record
          acquire-key-responses))

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

(defn acquire-unique-keys
  "attempts to acquire unique keys for an owner... returns
   a Deferred[Right[[:ok updated-owner-record :failed-keys]]] with an updated
   owner record containing only the keys that could be acquired"
  [session ^Model model new-record]
  (m/with-monad dm/either-deferred-monad
    (m/mlet [old-record (r/select-one session
                                      (get-in model [:primary-table :name])
                                      (get-in model [:primary-table :key])
                                      (t/extract-uber-key-value model new-record))

             release-key-responses (release-stale-unique-keys
                                    session
                                    model
                                    old-record
                                    new-record)
             acquire-key-responses (acquire-unique-keys
                                    session
                                    model
                                    new-record)]
            (m/return
             (update-record-by-key-responses
              old-record
              new-record
              acquire-key-responses)))))
