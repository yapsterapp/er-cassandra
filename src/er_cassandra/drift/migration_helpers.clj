(ns er-cassandra.drift.migration-helpers
  (:require [drift.config :as config]
            [er-cassandra.session]
            [clojure.java.shell :refer [sh]]
            [environ.core :refer [env]]))

(defn session
  "get the alia session"
  []
  (:er-cassandra-session config/*config-map*))

(defmacro execute
  [& body]
  `(deref
    (er-cassandra.session/execute (session) ~@body)))

(defn pause
  []
  (println "pausing to let cassandra catch up")
  (Thread/sleep 5000))

(defn cqlsh
  [cqlsh-cmd]
  (let [cqlsh (env :cqlsh "cqlsh")]
    (sh cqlsh :in cqlsh-cmd)))
