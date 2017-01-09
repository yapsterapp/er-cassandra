(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [trace debug info warn error]]
   [environ.core :refer [env]]
   [clojure.string :as str]
   [qbits.alia :as alia]
   [qbits.alia.policy.load-balancing :as lb]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [cats.core :refer [mlet return >>=]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
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
   (let [str-statement (if (string? statement)
                         statement
                         (h/->raw statement))]
     (when *trace*
       (debug str-statement))
     str-statement)
   opts))

(defn- execute-buffered*
  "execute a statement returning a Deferred<Stream<record>>"
  [alia-session statement opts]
  (return
   deferred-context
   (aliam/execute-buffered
    alia-session
    (let [str-statement (if (string? statement)
                          statement
                          (h/->raw statement))]
      (when *trace*
        (debug str-statement))
      str-statement)
    opts)))

(defn shutdown-session-and-cluster
  [session]
  (info "closing underlying alia session and cluster")
  (let [cluster (.getCluster session)]
    (alia/shutdown session)
    (alia/shutdown cluster)))

(defnk ^:private create-alia-session*
  "returns a Deferred<com.datastax.driver.core/Session>"
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
        alia-session (alia/connect cluster)

        init-fns (for [stmt init-statements]
                   (fn [_]
                     (as-> stmt %
                       (str/replace % "${keyspace}" keyspace)
                       (alia/execute alia-session %))
                     (return deferred-context true)))]

    (with-context deferred-context
      (mlet [_ (if (not-empty init-fns)
                 (apply >>= (return true) init-fns)
                 (return true))
             _ (alia/execute alia-session (str "USE " keyspace ";"))]
        (return alia-session)))))

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
    (shutdown-session-and-cluster alia-session)))

(defnk create-session
  [contact-points datacenter keyspace :as args]
  (with-context deferred-context
    (mlet [datastax-session (create-alia-session* args)
           :let [alia-session (->AliaSession
                               keyspace
                               datastax-session)]]
      (return
       [alia-session
        (fn [] (s/close alia-session))]))))

(defn mutated-tables
  [spy-session]
  (let [stmts (s/spy-log spy-session)]
    (->> stmts
         (filter map?) ;; don't blow up for manual stmts
         (map #(select-keys % [:insert
                               :update
                               :delete
                               :truncate]))
         (mapcat vals)
         (filter identity)
         distinct
         sort
         vec)))

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

    (info "truncating spy tables:" mts)

    (with-context deferred-context
      (mlet [:let [tfns (for [mt mts]
                          (fn [_]
                            (s/execute spy-session (h/truncate mt))
                            (return deferred-context true)))]

             _ (if (not-empty tfns)
                 (apply >>= (return true) tfns)
                 (return true))]

        (s/reset-spy-log spy-session)
        (return true)))))

(defrecord AliaSpySession [keyspace alia-session spy-log-atom truncate-on-close]
  Session
  (execute [this statement]
    (s/execute this statement {}))
  (execute [_ statement opts]
    (info "spying" statement opts)
    (swap! spy-log-atom conj statement)
    (execute* alia-session statement opts))
  (execute-buffered [this statement]
    (s/execute-buffered this statement {}))
  (execute-buffered [_ statement opts]
    (swap! spy-log-atom conj statement)
    (execute-buffered* alia-session statement opts))
  (close [self]
    (with-context deferred-context
      (mlet [_ (if truncate-on-close
                 (truncate-spy-tables self)
                 (return true))]
        (shutdown-session-and-cluster alia-session))))

  KeyspaceProvider
  (keyspace [_] keyspace)

  SpySession
  (spy-log [_] @spy-log-atom)
  (reset-spy-log [_] (reset! spy-log-atom [])))

(defnk create-spy-session
  [contact-points datacenter keyspace {truncate-on-close nil} :as args]
  (with-context deferred-context
    (mlet [datastax-session (create-alia-session* (dissoc args :truncate-on-close))
           :let [spy-session (->AliaSpySession
                              keyspace
                              datastax-session
                              (atom [])
                              truncate-on-close)]]
      (return
       [spy-session
        (fn [] (s/close spy-session))]))))

;; a test-session is just a spy-session which will initialise it's keyspace
;; if necesary and will truncate any used tables when closed
(defnk create-test-session
  [{contact-points nil} {port nil} {datacenter nil} keyspace :as args]
  (create-spy-session
   (merge
    args
    {:contact-points (or contact-points ["localhost"])
     :port (or port
               (some-> (env :cassandra-port) Integer/parseInt)
               9042)
     :datacenter datacenter
     :init-statements
     [(str "CREATE KEYSPACE IF NOT EXISTS "
           "  \"${keyspace}\" "
           "WITH replication = "
           "  {'class': 'SimpleStrategy', "
           "   'replication_factor': '1'} "
           " AND durable_writes = true;")]

     :truncate-on-close true})))
