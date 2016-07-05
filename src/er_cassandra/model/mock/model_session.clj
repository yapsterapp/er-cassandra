(ns er-cassandra.model.mock.model-session
  (:require
   [plumbing.core :refer :all]
   [er-cassandra.model.model-session :refer [ModelSession ModelSpySession]]))

(defprotocol MockModelSession
  (-check [this]))

(defrecord MatchMockModelSession []
  ModelSession
  (-record-session [_])
  (-select [_ model key record-or-key-value opts])
  (-upsert [_ model record opts])
  (-delete [_ model key record-or-key-value opts])
  (-close [_])

  MockModelSession
  (-check [_])

  ModelSpySession
  (-model-spy-log [_]))

(defn create-match-mock-model-session
  []
  (map->MatchMockModelSession
   {}))
