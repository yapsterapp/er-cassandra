(ns er-cassandra.session.mock
  (:require
   [manifold.deferred :as d])
  (:import
   [er_cassandra.session Session]))

(defn deferred-value
  [v]
  (let [d (d/deferred)]
    (if (instance? Throwable v)
      (d/error! d v)
      (d/success! d v))
    d))

(defrecord MockSession [statement-responses]
  Session
  (execute [_ statement]
    (if (contains? statement-responses statement)
      (deferred-value
        (get statement-responses statement))

      (throw (ex-info "no matching response" {:statement statement}))))
  (close [_]))

(defn create-session
  [statement-responses]
  (->MockSession statement-responses))
