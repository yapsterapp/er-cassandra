(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.session])
  (:import
   [er_cassandra.session Session SpySession]))

(def ^:dynamic debug nil)

(defmacro with-debug
  [& forms]
  `(binding [debug true]
     ~@forms))

(defn- execute*
  [alia-session statement]
  (aliam/execute
   alia-session
   (if (string? statement)
     statement
     (do
       (when debug
         (prn statement))
       (h/->raw statement)))))

(defn- create-alia-session*
  [contact-points keyspace port]
  (let [cluster (alia/cluster (assoc-when
                               {:contact-points contact-points}
                               :port port))
        alia-session (alia/connect cluster)]
    (alia/execute alia-session (str "USE " keyspace ";"))
    alia-session))

(defrecord AliaSession [keyspace alia-session]
  Session
  (execute [_ statement]
    (execute* alia-session statement))
  (close [_]
    (.close alia-session)))

(defnk create-session
  [contact-points keyspace {port nil}]
  (->AliaSession keyspace (create-alia-session* contact-points keyspace port)))

(defrecord AliaSpySession [keyspace alia-session spy-log-atom]
  Session
  (execute [_ statement]
    (swap! spy-log-atom conj statement)
    (execute* alia-session statement))
  (close [_]
    (.close alia-session))
  SpySession
  (spy-log [_] @spy-log-atom))

(defnk create-spy-session
  [contact-points keyspace {port nil}]
  (->AliaSpySession keyspace
                    (create-alia-session* contact-points keyspace port)
                    (atom [])))
