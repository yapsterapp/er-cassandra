(ns er-cassandra.session.alia
  (:require
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.session])
  (:import
   [er_cassandra.session Session]))

(defrecord AliaSession [alia-session]
  Session
  (execute [statement]
    (aliam/execute
     alia-session
     (h/->raw statement))))

(defn create-session
  [alia-session]
  (->AliaSession alia-session))
