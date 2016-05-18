(ns er-cassandra.session.alia
  (:require
   [plumbing.core :refer :all]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [er-cassandra.session])
  (:import
   [er_cassandra.session Session]))

(def ^:dynamic debug nil)

(defmacro with-debug
  [& forms]
  `(binding [debug true]
     ~@forms))

(defrecord AliaSession [keyspace alia-session]
  Session
  (execute [_ statement]
    (aliam/execute
     alia-session
     (if (string? statement)
       statement
       (do
         (when debug
           (prn statement))
         (h/->raw statement)))))
  (close [_]
    (.close alia-session)))

(defnk create-session
  [contact-points keyspace {port nil}]
  (let [cluster (alia/cluster {:contact-points contact-points
                               :port port})
        alia-session (alia/connect cluster)]
    (alia/execute alia-session (str "USE " keyspace ";"))
    (->AliaSession keyspace alia-session)))
