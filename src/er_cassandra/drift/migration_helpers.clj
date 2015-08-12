(ns er-cassandra.drift.migration-helpers
  (:require [drift.config :as config]
            [er-cassandra.session]))

(defn session
  "get the alia session"
  []
  (:er-cassandra-session config/*config-map*))

(defmacro execute
  [& body]
  `(er-cassandra.session/execute (session) ~@body))
