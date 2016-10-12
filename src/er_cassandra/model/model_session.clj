(ns er-cassandra.model.model-session
  (:require
   [er-cassandra.model.types])
  (:import
   [er_cassandra.model.types Model]))

(defprotocol ModelSession
  (-record-session [this]
    "return the low-level record session")

  (-select [this ^Model model key record-or-key-value opts]
    "select model instances")

  (-upsert [this ^Model model record opts]
    "upsert a single model instance")

  (-delete [this ^Model model key record-or-key-value opts]
    "delete a single model instance")

  (-close [this]
    "close the session, releasing any resources"))

(defprotocol ModelSpySession
  (-model-spy-log [this])
  (-reset-model-spy-log [this]))
