(ns er-cassandra.model.alia.delete
  (:require
   [cats.core :as m :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]

   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.unique-key :as alia-unique-key]
   [er-cassandra.model.alia.upsert :as alia-upsert]
   [er-cassandra.model.alia.select :as alia-select])
  (:import
   [er_cassandra.model.types Model]
   [er_cassandra.session Session]))

(defn nil-values
  "return a record with the same keys as m but nil values... used to
   force all secondary/lookup keys to be considered for stale deletion"
  [m]
  (->> m
       keys
       (map (fn [k] [k nil]))
       (into {})))

(defn ^:private delete-with-primary
  [^Session session ^Model model key record opts]
  (with-context deferred-context
    (mlet [primary-response (alia-upsert/delete-record
                             session
                             model
                             (:primary-table model)
                             (t/extract-uber-key-value
                              model
                              record))

           unique-responses (alia-unique-key/release-stale-unique-keys
                             session
                             model
                             record
                             (nil-values record))

           secondary-responses (alia-upsert/delete-stale-secondaries
                                session
                                model
                                record
                                (nil-values record))

           lookup-responses (alia-upsert/delete-stale-lookups
                             session
                             model
                             record
                             (nil-values record))]
      (return
       [:ok record :deleted]))))

(defn delete*
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([^Session session ^Model model key record-or-key-value opts]

   (with-context deferred-context
     (mlet [[record & _] (alia-select/select* session
                                              model
                                              key
                                              record-or-key-value
                                              nil)]
       (if record
         (delete-with-primary session model key record opts)
         (return
          [:ok nil :no-primary-record]))))))
