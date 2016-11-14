(ns er-cassandra.drift.migration-helpers
  (:require [drift.config :as config]
            [er-cassandra.session]
            [clojure.java.shell :refer [sh]]
            [environ.core :refer [env]]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [manifold.deferred :as d]
            [cats.labs.manifold :refer [deferred-context]]))

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
  (with-context deferred-context
    (mlet [{sh-exit :exit
            sh-out :out
            sh-err :err
            :as sh-r} (d/future
                           (sh (env :cqlsh "cqlsh")
                               :in cqlsh-cmd))]
      ;; cqlsh seems to return code 0 with a non-empty error
      ;; instead of non-zero error-codes
      (if (and (= 0 sh-exit)
               (empty? sh-err))
        (return sh-out)
        (throw (ex-info "sh error" {:cqlsh-cmd cqlsh-cmd
                                    :exit sh-exit
                                    :out sh-out
                                    :err sh-err}))))))
