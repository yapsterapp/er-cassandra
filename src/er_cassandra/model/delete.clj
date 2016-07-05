(ns er-cassandra.model.delete
  (:require
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]

   [er-cassandra.model.types :as t]
   [er-cassandra.model.util :refer [combine-responses]]
   [er-cassandra.model.unique-key :as unique-key]
   [er-cassandra.model.upsert :as upsert]
   [er-cassandra.model.select :as select]
   [er-cassandra.model.util :as util])
  (:import [er_cassandra.model.types Model]))

(defn delete
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([session ^Model model key record-or-key-value]

   (with-context deferred-context
     (mlet [record (select/select-one session
                                      model
                                      key
                                      record-or-key-value)

            primary-response (if record
                               (upsert/delete-record
                                session
                                model
                                (:primary-table model)
                                (t/extract-uber-key-value
                                 model
                                 record))
                               (return nil))

            unique-responses (if record
                               (unique-key/release-stale-unique-keys
                                session
                                model
                                record
                                nil)
                               (return nil))

            secondary-responses (if record
                                  (upsert/delete-stale-secondaries
                                   session
                                   model
                                   record
                                   nil)
                                  (return nil))

            lookup-responses (if record
                               (upsert/delete-stale-lookups
                                session
                                model
                                record
                                nil)
                               (return nil))]
       (return
        [:ok record :deleted])))))

(defn delete-many
  "issue one delete query for each record and combine the responses"
  [session ^Model model key records]
  (->> records
       (map (fn [record]
              (delete session model key record)))
       util/combine-responses))
