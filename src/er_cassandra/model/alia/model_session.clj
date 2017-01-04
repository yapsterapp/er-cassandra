(ns er-cassandra.model.alia.model-session
  (:require
   [plumbing.core :refer :all]
   [environ.core :refer [env]]
   [cats.core :refer [mlet return >>=]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.session :as s]
   [er-cassandra.session.alia :as a]
   [er-cassandra.model.model-session :as ms
    :refer [ModelSession ModelSpySession]]
   [er-cassandra.model.alia.select :refer [select*]]
   [er-cassandra.model.alia.select-buffered :refer [select-buffered*]]
   [er-cassandra.model.alia.upsert :refer [upsert*]]
   [er-cassandra.model.alia.delete :refer [delete*]])
  (:import
   [er_cassandra.model.types Entity]))

(defrecord AliaModelSession [alia-session]
  ModelSession
  (-record-session [_]
    alia-session)

  (-select [_ entity key record-or-key-value opts]
    (select* alia-session entity key record-or-key-value opts))

  (-select-buffered [_ entity opts]
    (select-buffered* alia-session entity opts))

  (-select-buffered [_ entity key record-or-key-value opts]
    (select-buffered* alia-session entity key record-or-key-value opts))

  (-upsert [_ entity record opts]
    (upsert* alia-session entity record opts))

  (-delete [_ entity key record-or-key-value opts]
    (delete* alia-session entity key record-or-key-value opts))

  (-close [_]
    (s/close alia-session))

  s/Session
  (execute [_ statement]
    (s/execute alia-session statement))
  (execute [_ statement opts]
    (s/execute alia-session statement opts))
  (execute-buffered [_ statement]
    (s/execute-buffered alia-session statement))
  (execute-buffered [_ statement opts]
    (s/execute-buffered alia-session statement opts))
  (close [_]
    (s/close alia-session)))

(defnk create-session
  [contact-points keyspace {port nil} :as args]
  (with-context deferred-context
    (mlet [[alia-session _] (a/create-session args)
           :let [model-session (->AliaModelSession alia-session)]]
      (return
       [model-session
        (fn [] (ms/-close model-session))]))))

(defrecord AliaModelSpySession [alia-session model-spy-log-atom]
  ModelSession
  (-record-session [_]
    alia-session)

  (-select [_ entity key record-or-key-value opts]
    (swap! model-spy-log-atom conj {:action :select
                                    :entity entity
                                    :key key
                                    :record-or-key-value record-or-key-value
                                    :opts opts})
    (select* alia-session entity key record-or-key-value opts))

  (-select-buffered [_ entity opts]
    (swap! model-spy-log-atom conj {:action :select
                                    :entity entity
                                    :opts opts})
    (select-buffered* alia-session entity opts))

  (-select-buffered [_ entity key record-or-key-value opts]
    (swap! model-spy-log-atom conj {:action :select
                                    :entity entity
                                    :key key
                                    :record-or-key-value record-or-key-value
                                    :opts opts})
    (select-buffered* alia-session entity key record-or-key-value opts))

  (-upsert [_ entity record opts]
    (swap! model-spy-log-atom conj {:action :upsert
                                    :entity entity
                                    :record record
                                    :opts opts})
    (upsert* alia-session entity record opts))

  (-delete [_ entity key record-or-key-value opts]
    (swap! model-spy-log-atom conj {:action :delete
                                    :entity entity
                                    :key key
                                    :record-or-key-value record-or-key-value
                                    :opts opts})
    (delete* alia-session entity key record-or-key-value opts))

  (-close [_]
    (with-context deferred-context
      (mlet [_ (s/close alia-session)]
        (swap! model-spy-log-atom (fn [_] []))
        (return true))))

  ModelSpySession
  (-model-spy-log [_] @model-spy-log-atom)
  (-reset-model-spy-log [_]
    (reset! model-spy-log-atom [])
    (s/reset-spy-log alia-session))

  s/Session
  (execute [_ statement]
    (s/execute alia-session statement))
  (execute [_ statement opts]
    (s/execute alia-session statement opts))
  (execute-buffered [_ statement]
    (s/execute-buffered alia-session statement))
  (execute-buffered [_ statement opts]
    (s/execute-buffered alia-session statement opts))
  (close [this]
    (ms/-close this))

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
  [contact-points keyspace {port nil} {truncate-on-close nil} :as args]
  (with-context deferred-context
    (mlet [[alia-session _] (a/create-spy-session args)
           :let [spy-session (->AliaModelSpySession alia-session (atom []))]]
      (return
       [spy-session
        (fn [] (ms/-close spy-session))]))))

(defnk create-test-session
  [{contact-points nil} {port nil} {datacenter nil} keyspace :as args]
  (with-context deferred-context
    (mlet [[alia-test-session _] (a/create-test-session args)
           :let [model-test-session (->AliaModelSpySession alia-test-session (atom []))]]
      (return
       [model-test-session
        (fn [] (ms/-close model-test-session))]))))
