(ns er-cassandra.model.model-session
  (:require
   [qbits.alia.codec :as ac]
   [er-cassandra.model.types])
  (:import
   [er_cassandra.model.types Entity]))

(defprotocol ModelSession
  (-record-session [this]
    "return the low-level record session")

  (-select [this ^Entity entity key record-or-key-value opts]
    "select entity instances")

  (-select-buffered
    [this ^Entity entity opts]
    [this ^Entity entity key record-or-key-value opts]
    "select entity instances, returning a stream of results")

  (-upsert [this ^Entity entity record opts]
    "upsert a single entity instance")

  (-delete [this ^Entity entity key record-or-key-value opts]
    "delete a single entity instance")

  (-close [this]
    "close the session, releasing any resources"))

(defprotocol ModelSpySession
  (-model-spy-log [this])
  (-reset-model-spy-log [this]))

;; tags records retrieved from the db
(defrecord EntityInstance [])

(defn entity-instance?
  "tests if the record was retrieved from the db"
  [r]
  (instance? EntityInstance r))

;; a RowGenerator which tags the record with EntityInstance
(deftype EntityInstanceRowGenerator []
  ac/RowGenerator
  (init-row [_] (transient {}))
  (conj-row [_ row k v] (assoc! row (keyword k) v))
  (finalize-row [_ row] (map->EntityInstance (persistent! row))))

;; print the ModelInstance records as vanilla maps for now
(defmethod print-method er_cassandra.model.model_session.EntityInstance [x writer]
  (print-method (into {} x) writer))
