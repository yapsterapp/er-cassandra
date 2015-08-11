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
   [er-cassandra.record :as r]
   [er-cassandra.record-either :as re]
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
      (m/mlet [insert-response (re/insert session
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
                           (re/select-one session
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
                                       (re/update
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

(defn remove-unique-key
  "remove a single unique key"
  [session ^Model model unique-key-table uber-key-value key-value]
  (let [uber-key (t/uber-key model)
        key (:key unique-key-table)]
    (m/with-monad dm/either-deferred-monad
      (m/mlet [delete-result (re/delete session
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
                 deleted? [:ok :deleted]
                 :else    [:ok :stale]))))))

(defn acquire-singular-unique-key
  "attempt to acquire a singular unique key, returning a
   Deferred[Right[[:ok key info]]] or a Deferred[Right[[:fail key reason]]]"
  [session ^Model model unique-key-table owner-record]
  (let [uber-key-value (t/extract-uber-key-value model owner-record)
        key-value (k/extract-key-value (:key unique-key-table) owner-record)]
    (acquire-unique-key session model unique-key-table uber-key-value key-value)))

(defn acquire-unique-key-collection
  "attempt to acquire a set of unique keys. returns a
   Deferred[Right[[ [:ok key info] | [:fail key reason] ]]]"
  [session ^Model model unique-key-table owner-record]
  (let [uber-key-value (t/extract-uber-key-value model owner-record)
        key-value-coll (k/extract-key-value-collection
                        (:key unique-key-table)
                        owner-record)
        responses (mapv (partial acquire-unique-key
                                 session
                                 model
                                 unique-key-table
                                 uber-key-value)
                        key-value-coll)]
    (combine-responses responses)))


(defn acquire-unique-keys
  "attempts to acquire unique keys for an owner... returns
   a Deferred[Right[[:ok updated-owner-record :failed-keys]]] with an updated
   owner record containing only the keys that could be acquired"
  [session ^Model model owner-record]
  (m/with-monad dm/either-deferred-monad
    (m/mlet [r owner-record]
            (m/return r))))
