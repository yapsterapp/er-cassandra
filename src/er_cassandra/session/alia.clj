(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.string :as str]
   [qbits.alia :as alia]
   [qbits.alia.policy.load-balancing :as lb]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.session :as s])
  (:import
   [er_cassandra.session
    Session
    SpySession
    KeyspaceProvider]))

(def ^:dynamic *trace* nil)

(defmacro with-debug
  [& forms]
  `(binding [*trace* true]
     ~@forms))

(defn- execute*
  [alia-session statement opts]
  (aliam/execute
   alia-session
   (do
     (when *trace*
       (debug statement))
     (if (string? statement)
       statement
       (h/->raw statement)))
   opts))

(defn- execute-buffered*
  [alia-session statement opts]
  (aliam/execute-buffered
   alia-session
   (do
     (when *trace*
       (debug statement))
     (if (string? statement)
       statement
       (h/->raw statement)))
   opts))

(defnk ^:private create-alia-session*
  [contact-points
   datacenter
   keyspace
   {init-statements nil}
   :as args]
  (let [cluster-args (dissoc args :keyspace :datacenter :init-statements)
        cluster-args (assoc-when
                      cluster-args
                      :load-balancing-policy (when datacenter
                                               (lb/dc-aware-round-robin-policy
                                                datacenter)))

        _ (info "create-alia-session*" args)
        cluster (alia/cluster cluster-args)
        alia-session (alia/connect cluster)]

    (when (not-empty init-statements)
      (doseq [stmt init-statements]
        (as-> stmt %
          (str/replace % "${keyspace}" keyspace)
          (alia/execute alia-session %))))

    (alia/execute alia-session (str "USE " keyspace ";"))

    alia-session))

(defrecord AliaSession [keyspace alia-session]
  Session
  (execute [_ statement]
    (execute* alia-session statement {}))
  (execute [_ statement opts]
    (execute* alia-session statement opts))
  (execute-buffered [_ statement]
    (execute-buffered* alia-session statement {}))
  (execute-buffered [_ statement opts]
    (execute-buffered* alia-session statement opts))
  (close [_]
    (.close alia-session)))

(defnk create-session
  [contact-points datacenter keyspace :as args]
  (->AliaSession
   keyspace
   (create-alia-session* args)))

(defn mutated-tables
  [spy-session]
  (let [stmts (s/spy-log spy-session)]
    (->> stmts
         (map #(select-keys % [:insert
                               :update
                               :delete
                               :truncate]))
         (mapcat vals)
         (filter identity)
         distinct)))

(defn truncate-spy-tables
  "given a spy session, truncate all tables which have been accessed,
   then reset the spy-log"
  [spy-session]
  (let [ks (s/keyspace spy-session)
        mts (mutated-tables spy-session)]
    (when-not (str/ends-with? (name ks) "_test")
      (throw (ex-info "truncate-spy-tables is only for *_test keyspaces"
                      {:keyspace ks
                       :mutated-tables mts})))
    (doseq [mt mts]
      (s/execute spy-session (h/truncate mt)))
    (s/reset-spy-log spy-session)))

(defrecord AliaSpySession [keyspace alia-session spy-log-atom truncate-on-close]
  Session
  (execute [this statement]
    (s/execute this statement {}))
  (execute [_ statement opts]
    (swap! spy-log-atom conj statement)
    (execute* alia-session statement opts))
  (execute-buffered [this statement]
    (s/execute-buffered this statement {}))
  (execute-buffered [_ statement opts]
    (swap! spy-log-atom conj statement)
    (execute-buffered* alia-session statement opts))
  (close [self]
    (when truncate-on-close
      (truncate-spy-tables self))
    (.close alia-session))

  KeyspaceProvider
  (keyspace [_] keyspace)

  SpySession
  (spy-log [_] @spy-log-atom)
  (reset-spy-log [_] (reset! spy-log-atom [])))

(defnk create-spy-session
  [contact-points datacenter keyspace {truncate-on-close nil} :as args]
  (->AliaSpySession
   keyspace
   (create-alia-session* (dissoc args :truncate-on-close))
   (atom [])
   truncate-on-close))

(defn with-spy-session-reset*
  "start an alia spy-session, call a function and truncate
  all tables which were touched during the function"
  [spy-session f]
  (try
    (f)
    (finally
      (truncate-spy-tables spy-session))))

(defmacro with-spy-session-reset
  [spy-session & body]
  `(with-spy-session-reset*
     ~spy-session
     (fn [] ~@body)))
