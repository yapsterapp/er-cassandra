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

(defrecord MapMockSession [statement-responses statement-log-atom]
  Session
  (execute [_ statement]
    (swap! statement-log-atom conj statement)
    (if (contains? statement-responses statement)
      (deferred-value
        (get statement-responses statement))

      (throw (ex-info "no matching response" {:statement statement}))))
  (close [_]))

(defn create-map-mock-session
  [statement-responses]
  (->MapMockSession statement-responses (atom [])))

;; need a statement-matcher, not just a literal statement...
;; maybe there should be a bunch of "query matchers" and
;; one or more expectation statements which have to be given
;; for the test to pass
(defrecord ListMockSession [statement-responses statement-log-atom]
  Session
  (execute [_ statement]
    (let [i (count @statement-log-atom)]
      (swap! statement-log-atom conj statement)

      (if (< i (count statement-responses))
        (let [[st r] (nth statement-responses i)]

          (if (= st statement)
            (deferred-value r)

            (throw (ex-info "statment match failed"
                            {:statement statement
                             :expected-statement st
                             :expected-response r
                             :statement-responses statement-responses
                             :statement-log @statement-log-atom}))))

        (throw (ex-info "too many statements"
                        {:statement statement
                         :statement-responses statement-responses})))))

  (close [_]
    (when (< (count @statement-log-atom)
             (count statement-responses))
      (throw (ex-info "more statements expected"
                      {:statement-responses statement-responses})))))

(defn create-list-mock-session
  [statement-responses]
  (->ListMockSession statement-responses (atom [])))
