(ns er-cassandra.model.delete
  (:require
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.util :refer [combine-responses]])
  (:import
   [er_cassandra.model.types Model]
   [er_cassandra.model.model_session ModelSession]))

(defn delete
  "delete a single instance, removing primary, secondary unique-key and
   lookup records "

  ([^ModelSession session ^Model model key record-or-key-value]
   (delete session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]

   (ms/-delete session model key record-or-key-value opts)))

(defn delete-many
  "issue one delete query for each record and combine the responses"
  [^ModelSession session ^Model model key records]
  (->> records
       (map (fn [record]
              (delete session model key record)))
       combine-responses))
