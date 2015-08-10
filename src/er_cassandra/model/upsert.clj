(ns er-cassandra.model.upsert
  (:require
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
   [er-cassandra.model.types :as t])
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

(defn unique-key-table-upsert
  "insert into a unique-key table with a lightweight transaction.
   returns a Deferred[Either], with Right values describing how
   they were successful, and Left values describing errors"

  [session ^Model model unique-key-table unique-key-record]

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
               inserted? [:ok :inserted]    ;; new key
               owned?    [:ok :owned]       ;; ours already
               updated?  [:ok :updated]     ;; ours now
               live-ref? [:fail :notunique] ;; not ours
               :else     [:fail :notunique] ;; someone else won
               )))))

(defn acquire-unique-key
  "acquire a single unique key.
   returns a Deferred[Right[:ok <keydesc> info]] if the key was acquired
   successfully, a Deferred[Right[:fail <keydesc> reason]]"
  [session ^Model model unique-key-table uber-key-value key-value]
  (let [uber-key (t/uber-key model)
        key (:key unique-key-table)
        unique-key-record (into {}
                                (concat (map vector uber-key uber-key-value)
                                        (map vector key key-value)))]
    (m/with-monad dm/either-deferred-monad
      (m/mlet [acquire-result (unique-key-table-upsert session
                                                       model
                                                       unique-key-table
                                                       unique-key-record)]
              (m/return
               (match acquire-result
                      [:ok info] [:ok
                                  {:uber-key uber-key
                                   :uber-key-value uber-key-value
                                   :key key
                                   :key-value key-value}
                                  info]
                      [_ reason] [:fail
                                  {:uber-key uber-key
                                   :uber-key-value uber-key-value
                                   :key key
                                   :key-value key-value}
                                  reason]))))))

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
    (d/chain
     (apply d/zip responses)
     (fn [responses]
       (clojure.pprint/pprint responses)
       (if (some either/left? responses)
         (either/left responses)

         (either/right (mapv deref responses)))))))

(defn lookup-key-table-upsert
  [session ^Model model lookup-key-table record]
  (m/with-monad dm/either-deferred-monad
    (m/mlet [upsert-response (re/insert session
                                        (:name lookup-key-table)
                                        record)]
            (m/return :upserted))))






(defn upsert
  "upsert a single instance, creating primary, secondary, unique-key and
   lookup records as required"

  ([session ^Model model record]
   (upsert session model record {}))

  ([session ^Model model record opts]
   ;; insert to primary table if not exists
   ;; insert to secondary tables if not exists
   ;; insert to each lookup-table if not exists
   ))
