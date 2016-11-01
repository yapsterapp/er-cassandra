(ns er-cassandra.model.model-session
  (:require
   [qbits.alia.codec :as ac]
   [er-cassandra.model.types])
  (:import
   [er_cassandra.model.types Model]))

(defprotocol ModelSession
  (-record-session [this]
    "return the low-level record session")

  (-select [this ^Model model key record-or-key-value opts]
    "select model instances")

  (-select-buffered [this ^Model model key record-or-key-value opts]
    "select model instances, returning a stream of results")

  (-upsert [this ^Model model record opts]
    "upsert a single model instance")

  (-delete [this ^Model model key record-or-key-value opts]
    "delete a single model instance")

  (-close [this]
    "close the session, releasing any resources"))

(defprotocol ModelSpySession
  (-model-spy-log [this])
  (-reset-model-spy-log [this]))

;; tags records retrieved from the db
(defrecord ModelInstance [])

(defn model-instance?
  "tests if the record was retrieved from the db"
  [r]
  (instance? ModelInstance r))

;; a RowGenerator which tags the record with ModelInstance
(deftype ModelInstanceRowGenerator []
  ac/RowGenerator
  (init-row [_] (transient {}))
  (conj-row [_ row k v] (assoc! row (keyword k) v))
  (finalize-row [_ row] (map->ModelInstance (persistent! row))))

;; print the ModelInstance records as vanilla maps for now
(defmethod print-method er_cassandra.model.model_session.ModelInstance [x writer]
  (print-method (into {} x) writer))
