(ns er-cassandra.model.delete
  (:require
   [clojure.set :as set]
   [clojure.core.match :refer [match]]
   [manifold.deferred :as d]
   [cats.core :as m]
   [cats.monad.deferred :as dm]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.key :as k]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.util :refer [combine-responses create-lookup-record]]
   [er-cassandra.model.unique-key :as unique-key]
   [er-cassandra.model.upsert :as upsert]
   [er-cassandra.model.select :as select])
  (:import [er_cassandra.model.types Model]))

(defn delete
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([session ^Model model key record-or-key-value]

   (m/with-monad dm/deferred-monad
     (m/mlet [record (select/select-one session
                                        model
                                        key
                                        record-or-key-value)

              primary-response (upsert/delete-record
                                session
                                model
                                (:primary-table model)
                                (t/extract-uber-key-value model record))

              unique-responses (unique-key/release-stale-unique-keys
                                session
                                model
                                record
                                nil)

              secondary-responses (upsert/delete-stale-secondaries
                                   session
                                   model
                                   record
                                   nil)

              lookup-responses (upsert/delete-stale-lookups
                                session
                                model
                                record
                                nil)]
             (m/return
              [:ok record :deleted])))))
