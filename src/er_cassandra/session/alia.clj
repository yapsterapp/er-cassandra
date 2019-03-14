(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [log trace debug info warn error]]
   [environ.core :refer [env]]
   [clojure.string :as str]
   [qbits.alia :as alia]
   [qbits.alia.policy.load-balancing :as lb]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [cats.core :as monad :refer [mlet return >>=]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [manifold.deferred :as d]
   [er-cassandra.session :as s]
   [prpr.promise :as pr :refer [ddo]])
  (:import
   [er_cassandra.session
    Session
    SpySession
    KeyspaceProvider]))

(defn dissoc-prepared-stmt-if-error
  [stmt-cache str-statement]
  (let [pps (get stmt-cache str-statement)
        e? (some-> pps (d/error-value nil) some?)]
    (if e?
      (dissoc stmt-cache str-statement)
      stmt-cache)))

(defn prepare-async-via-agent
  [agent-state agent-ref alia-session str-statement deferred-result]
  (if-let [pps (get agent-state str-statement)]
    (do
      (trace "connecting in-flight prepared statement:" str-statement)
      (d/connect pps deferred-result)
      agent-state)
    (do
      (trace "preparing statement:" str-statement)
      (alia/prepare-async
       alia-session
       str-statement
       {:success (fn [ps]
                   (d/success! deferred-result ps))
        :error (fn [e]
                 (send
                  agent-ref
                  dissoc-prepared-stmt-if-error
                  str-statement)
                 (d/error! deferred-result e))})
      (assoc agent-state str-statement deferred-result))))

(defn maybe-prepare
  [alia-session
   prepared-stmts-agent
   str-statement]
  (if-let [pps (get @prepared-stmts-agent str-statement)]
    (do
      (trace "re-using prepared statement:" str-statement)
      pps)
    (let [r (d/deferred)]
      (send
       prepared-stmts-agent
       prepare-async-via-agent
       prepared-stmts-agent
       alia-session
       str-statement
       r)
      r)))

(defn- execute*
  [alia-session
   prepared-stmts-agent
   statement
   {values :values
    trace? :trace?
    prepare? :prepare?
    :as opts}]
  (ddo [:let [str-statement (if (string? statement)
                              statement
                              (h/->raw statement))]
        exec-statement (if prepare?
                         (maybe-prepare alia-session
                                        prepared-stmts-agent
                                        str-statement)
                         (return str-statement))]
    (when trace?
      (if (true? trace?)
        (debug [str-statement opts])
        (log trace? [str-statement opts])))
    (aliam/execute
     alia-session
     exec-statement
     (-> opts
         (dissoc :trace? :prepare?)))))

(defn- execute-buffered*
  "execute a statement returning a Deferred<Stream<record>>"
  [alia-session
   prepared-stmts-agent
   statement
   {values :values
    trace? :trace?
    prepare? :prepare?
    :as  opts}]
  (ddo [:let [str-statement (if (string? statement)
                              statement
                              (h/->raw statement))]
        exec-statement (if prepare?
                         (maybe-prepare alia-session
                                        prepared-stmts-agent
                                        str-statement)
                         (return str-statement))]
    (when trace?
      (if (true? trace?)
        (debug [str-statement opts])
        (log trace? [str-statement opts])))

    (return
     (aliam/execute-buffered
      alia-session
      exec-statement
      (-> opts
          (dissoc :trace? :prepare?))))))

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
                       (alia/execute alia-session % {}))
                     (return deferred-context true)))]

    (with-context deferred-context
      (mlet [_ (if (not-empty init-fns)
                 (apply >>= (return true) init-fns)
                 (return true))
             _ (alia/execute alia-session (str "USE " keyspace ";") {})]
        (return alia-session)))))

(defrecord AliaSession [keyspace alia-session prepared-stmts-agent trace?]
  KeyspaceProvider
  (keyspace [_] keyspace)
  Session
  (execute [_ statement opts]
    (execute* alia-session prepared-stmts-agent statement (assoc opts :trace? trace?)))
  (execute-buffered [_ statement opts]
    (execute-buffered* alia-session prepared-stmts-agent statement (assoc opts :trace? trace?)))
  (close [_]
    (shutdown-session-and-cluster alia-session)))

(defnk create-session
  [contact-points datacenter keyspace {trace? false}:as args]
  (with-context deferred-context
    (mlet
      [datastax-session (create-alia-session*
                         (dissoc args :trace?))
       :let [alia-session (map->AliaSession
                           {:keyspace keyspace
                            :alia-session datastax-session
                            :trace? trace?
                            :prepared-stmts-agent (agent {})})]]
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
         (map name) ;; coerce kw to string
         distinct
         sort
         vec)))

(defn truncate-test-tables
  "given a session truncate the tables given. only works in _test
   keyspaces"
  [session & tables]
  (let [ks (s/keyspace session)
        tables (filterv #(not= "schema_migrations" (some-> % name)) tables)]

    (when-not (str/ends-with? (name ks) "_test")
      (throw (ex-info "truncate-test-tables is only for *_test keyspaces"
                      {:keyspace ks
                       :tables tables})))

    (info "truncating test tables:" tables)

    (with-context deferred-context
      (mlet [:let [tfns (for [t tables]
                          (fn [_]
                            (s/execute session (h/truncate t) {})
                            (return deferred-context true)))]]
        (if (not-empty tfns)
          (apply >>= (return true) tfns)
          (return true))))))

(defn truncate-spy-tables
  "given a spy session, truncate all tables which have been accessed,
   then reset the spy-log"
  [spy-session]
  (with-context deferred-context
    (mlet [:let [mts (mutated-tables spy-session)]

           _ (apply truncate-test-tables spy-session mts)]

      (s/reset-spy-log spy-session)
      (return true))))

(defrecord AliaSpySession [keyspace
                           alia-session
                           prepared-stmts-agent
                           spy-log-atom
                           truncate-on-close
                           trace?]
  Session
  (execute [_ statement opts]
    (swap! spy-log-atom conj statement)
    (execute* alia-session prepared-stmts-agent statement (assoc opts :trace? trace?)))
  (execute-buffered [_ statement opts]
    (swap! spy-log-atom conj statement)
    (execute-buffered* alia-session prepared-stmts-agent statement (assoc opts :trace? trace?)))
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
  [contact-points
   datacenter
   keyspace
   {truncate-on-close nil}
   {trace? nil}
   :as args]
  (with-context deferred-context
    (mlet [datastax-session (create-alia-session*
                             (dissoc args
                                     :truncate-on-close
                                     :trace?))
           :let [spy-session (map->AliaSpySession
                              {:keyspace keyspace
                               :alia-session datastax-session
                               :spy-log-atom (atom [])
                               :truncate-on-close truncate-on-close
                               :trace? trace?
                               :prepared-stmts-agent (agent {})})]]
      (return
       [spy-session
        (fn [] (s/close spy-session))]))))

;; a test-session is just a spy-session which will initialise it's keyspace
;; if necesary and will truncate any used tables when closed
(defnk create-test-session
  [{contact-points nil}
   {port nil}
   {datacenter nil}
   {truncate-on-close nil}
   keyspace
   {trace? nil}
   :as args]
  (create-spy-session
   (merge
    args
    {:contact-points (or (some->
                          (env :cassandra-contact-points)
                          (str/split #","))
                         (some->
                          (env :cassandra-host)
                          (str/split #","))
                         ["localhost"])
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

     :truncate-on-close (if (nil? truncate-on-close)
                          true
                          truncate-on-close)
     :trace? trace?})))
