(ns er-cassandra.model.delete
  (:require
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.util :refer [combine-responses]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn delete
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (delete session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]

   (ms/-delete session entity key record-or-key-value opts)))

(defn delete-many
  "issue one delete query for each record and combine the responses"
  [^ModelSession session ^Entity entity key records]
  (->> records
       (map (fn [record]
              (delete session entity key record)))
       combine-responses))
