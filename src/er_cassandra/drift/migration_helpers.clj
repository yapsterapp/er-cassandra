(ns er-cassandra.drift.migration-helpers
  (:require [drift.config :as config]
            [qbits.alia]))

(defn alia-session
  "get the alia session"
  []
  (:alia-session config/*config-map*))

(defmacro execute
  [& body]
  `(qbits.alia/execute (alia-session) ~@body))
