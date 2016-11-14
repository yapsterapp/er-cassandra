(ns er-cassandra.drift.migrate-config
  (:require
   [plumbing.core :refer :all]
   [er-cassandra.drift.migrations :as m]))

(defnk cassandra
  [session keyspace namespace directory]

  {:directory directory
   :init (m/create-init-fn session keyspace namespace)
   :current-version m/current-version
   :update-version m/update-version
   :ns-content m/ns-content
   :finished m/finished})
