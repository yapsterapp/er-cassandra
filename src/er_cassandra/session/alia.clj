(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.session])
  (:import
   [er_cassandra.session Session]))

(defrecord AliaSession [alia-session]
  Session
  (execute [_ statement]
    (aliam/execute
     alia-session
     (h/->raw statement)))
  (close [_]
    (.close alia-session)))

(defnk create-session
  [contact-points keyspace]
  (let [cluster (alia/cluster {:contact-points contact-points})
        alia-session (alia/connect cluster)]
    (alia/execute alia-session (str "USE " keyspace ";"))
    (->AliaSession alia-session)))
