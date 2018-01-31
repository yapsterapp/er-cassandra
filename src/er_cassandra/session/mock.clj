(ns er-cassandra.session.mock
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.pprint :refer [pprint]]
   [manifold.deferred :as d]
   [er-cassandra.session :as s])
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
  (execute [_ statement opts]
    (swap! statement-log-atom conj statement)
    (if (contains? statement-responses statement)
      (deferred-value
        (get statement-responses statement))

      (throw (ex-info "no matching response" {:statement statement}))))
  (close [_]))

(defn create-map-mock-session
  [statement-responses]
  (->MapMockSession statement-responses (atom [])))

(defn requirement-match-unordered
  [requirement-matchers statement]
  (let [i-rms (keep-indexed vector requirement-matchers)
        [i-r resp] (some (fn [[i rm]]
                           (when-let [r (rm statement)]
                             [i r]))
                         i-rms)]
    (when i-r
      [(->> i-rms
            (remove (fn [[i rm]] (not= i i-r)))
            (map last))
       resp])))

(defn requirement-match-ordered
  [requirement-matchers statement]
  (let [[frm & rem] requirement-matchers
        resp (frm statement)]
    (when resp
      [rem resp])))

(defn find-requirement-match
  "match required statements
  returns [updated-requirement-matchers response] or nil
  if there was no match"
  [requirement-matchers statement]
  (if (vector? requirement-matchers)
    (requirement-match-ordered requirement-matchers statement)
    (requirement-match-unordered requirement-matchers statement)))

(defn find-support-match
  "match supporting statements
   returns response or nil"
  [support-matchers statement]
  (some (fn [sm] (sm statement))
        support-matchers))

;; need a statement-matcher, not just a literal statement...
;; maybe there should be a bunch of "query matchers" and
;; one or more expectation statements which have to be given
;; for the test to pass
(defrecord MatchMockSession [support-matchers
                             requirement-matchers-atom
                             statement-log-atom
                             response-log-atom
                             print?]
  Session
  (execute [_ statement opts]
    (swap! statement-log-atom conj statement)

    (if-let [[urms resp] (find-requirement-match
                          @requirement-matchers-atom
                          statement)]
      (do
        (reset! requirement-matchers-atom urms)
        (swap! response-log-atom conj resp)
        (when print? (pprint ["requirement match" statement]))
        (deferred-value resp))

      (if-let [resp (find-support-match support-matchers statement)]
        (do
          (swap! response-log-atom conj resp)
          (when print? (pprint ["support match" statement]))
          (deferred-value resp))

        (do
          (when print? (pprint ["failed match" statement]))
          (throw (ex-info "statment match failed"
                          {:statement statement
                           :statement-log @statement-log-atom}))))))

  (execute-buffered [this statement opts]
    (throw (ex-info "not implemented" {})))

  (close [_]
    (when (not-empty @requirement-matchers-atom)
      (throw (ex-info "some required statements not executed"
                      {:statement-log @statement-log-atom
                       :response-log @response-log-atom})))))

(defnk create-match-mock-session
  [{support-matchers nil} {requirement-matchers nil} {print? false}]
  (map->MatchMockSession
   {:support-matchers (flatten support-matchers)
    :requirement-matchers-atom (atom (flatten requirement-matchers))
    :statement-log-atom (atom [])
    :response-log-atom (atom [])
    :print? print?}))
