(ns er-cassandra.model.model-session
  (:require
   [er-cassandra.model.types])
  (:import
   [er_cassandra.model.types Model]))

(defprotocol ModelSession
  (-record-session [this])
  (-select [this ^Model model key record-or-key-value opts])
  (-upsert [this ^Model model record opts])
  (-delete [this ^Model model key record-or-key-value opts])
  (-close [this]))

(defprotocol ModelSpySession
  (-model-spy-log [this])
  (-reset-model-spy-log [this]))
