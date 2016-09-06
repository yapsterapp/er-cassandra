(ns er-cassandra.model.alia.model-session
  (:require
   [plumbing.core :refer :all]
   [er-cassandra.session :as s]
   [er-cassandra.session.alia :as a]
   [er-cassandra.model.model-session :as model-session
    :refer [ModelSession ModelSpySession]]
   [er-cassandra.model.alia.select :refer [select*]]
   [er-cassandra.model.alia.upsert :refer [upsert*]]
   [er-cassandra.model.alia.delete :refer [delete*]])
  (:import
   [er_cassandra.model.types Model]))

(defrecord AliaModelSession [alia-session]
  ModelSession
  (-record-session [_]
    alia-session)

  (-select [_ model key record-or-key-value opts]
    (select* alia-session model key record-or-key-value opts))

  (-upsert [_ model record opts]
    (upsert* alia-session model record opts))

  (-delete [_ model key record-or-key-value opts]
    (delete* alia-session model key record-or-key-value opts))

  (-close [_]
    (s/close alia-session))

  s/Session
  (execute [_ statement]
    (s/execute alia-session statement))
  (close [_]
    (s/close alia-session)))

(defnk create-session
  [contact-points keyspace {port nil} :as args]
  (let [alia-session (a/create-session args)]
    (->AliaModelSession alia-session)))

(defrecord AliaModelSpySession [alia-session model-spy-log-atom]
  ModelSession
  (-record-session [_]
    alia-session)

  (-select [_ model key record-or-key-value opts]
    (swap! model-spy-log-atom conj {:action :select
                                    :model model
                                    :key key
                                    :record-or-key-value record-or-key-value
                                    :opts opts})
    (select* alia-session model key record-or-key-value opts))

  (-upsert [_ model record opts]
    (swap! model-spy-log-atom conj {:action :upsert
                                    :model model
                                    :record record
                                    :opts opts})
    (upsert* alia-session model record opts))

  (-delete [_ model key record-or-key-value opts]
    (swap! model-spy-log-atom conj {:action :delete
                                    :model model
                                    :key key
                                    :record-or-key-value record-or-key-value
                                    :opts opts})
    (delete* alia-session model key record-or-key-value opts))

  (-close [_]
    (s/close alia-session))

  ModelSpySession
  (-model-spy-log [_] @model-spy-log-atom)
  (-reset-model-spy-log [_]
    (reset! model-spy-log-atom [])
    (s/reset-spy-log alia-session))

  s/Session
  (execute [_ statement]
    (s/execute alia-session statement))
  (close [_]
    (s/close alia-session))

  s/KeyspaceProvider
  (keyspace [_]
    (s/keyspace alia-session))

  s/SpySession
  (spy-log [_]
    (s/spy-log alia-session))
  (reset-spy-log [_]
    (reset! model-spy-log-atom [])
    (s/reset-spy-log alia-session)))

(defnk create-spy-session
  [contact-points keyspace {port nil} :as args]
  (let [alia-session (a/create-spy-session args)]
    (->AliaModelSpySession alia-session (atom []))))
