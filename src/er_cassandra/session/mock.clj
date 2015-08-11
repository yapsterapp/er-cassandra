(ns er-cassandra.session.mock
  (:import
   [er_cassandra.session Session]))

(defrecord MockSession [statement-responses]
  Session
  (execute [statement]
    (if (contains? statement-responses statement)
      (get statement-responses statement)

      (throw (ex-info "no matching response" {:statement statement})))))

(defn create-session
  [statement-responses]
  (->MockSession statement-responses))
