(ns er-cassandra.drift.migration-helpers
  (:require [drift.config :refer [*config-map*]]
            [er-cassandra.session]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [environ.core :refer [env]]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [manifold.deferred :as d]
            [cats.labs.manifold :refer [deferred-context]]))

(defn config
  [k]
  (get *config-map* k))

(defn session
  "get the alia session"
  []
  (config :er-cassandra-session))

(defmacro execute
  [& body]
  `(deref
    (er-cassandra.session/execute (session) ~@body)))

(defn pause
  []
  (println "pausing to let cassandra catch up")
  (Thread/sleep 5000))

(defn cqlsh
  "execute a command with cqlsh"
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

(defn empty-file
  "write or re-write f as a 0-length file and return Deferred<\"\">"
  [f]
  (let [fh (io/file f)]
    (with-open [out (io/writer fh)]
      (with-context deferred-context
        (return "")))))

(defn maybe-cqlsh
  "if :skip-cqlsh is defined return the result of calling alt-fn
   otherwise try running cqlsh with the given command"
  ([cqlsh-cmd]
   (maybe-cqlsh cqlsh-cmd nil))
  ([cqlsh-cmd alt-fn]
   (cond
     (config :skip-cqlsh)
     (if alt-fn
       (alt-fn)
       (with-context deferred-context
         (return "")))

     :else
     (cqlsh cqlsh-cmd))))
