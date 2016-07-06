(ns er-cassandra.model.mock.model-session
  (:require
   [plumbing.core :refer :all]
   [er-cassandra.model.model-session
    :refer [ModelSession ModelSpySession]]
   [er-cassandra.model.mock.mock-model-session :as mms
    :refer [MockModelSession MockModelSpySession Matcher
            -match -failure]]))

(defn- match
  [matchers request]
  (if-let [response (some #(-match % request) matchers)]
    response
    (throw (ex-info "no match" request))))

(defrecord MatchMockModelSession [matchers request-response-log-atom]
  ModelSession
  (-record-session [_])

  (-select [_ model key record-or-key-value opts]
    (let [request {:action :select
                   :model model
                   :key key
                   :record-or-key-value record-or-key-value
                   :opts opts}
          response (match matchers request)]
      (swap! request-response-log-atom conj {:request request :response response})
      response))

  (-upsert [_ model record opts]
    (let [request {:action :upsert
                   :model model
                   :record record
                   :opts opts}
          response (match matchers request)]
      (swap! request-response-log-atom conj {:request request :response response})
      response))

  (-delete [_ model key record-or-key-value opts]
    (let [request {:action :delete
                   :model model
                   :record-or-key-value record-or-key-value
                   :opts opts}
          response (match matchers request)]
      (swap! request-response-log-atom conj {:request request :response response})
      response))

  (-close [_])

  MockModelSession
  (-check [_]
    (let [failures (->> matchers
                        (map -failure)
                        (filter identity))]
      (when-not (empty? failures)
        (throw (ex-info "failed matchers" {:failures failures })))))


  ModelSpySession
  (-model-spy-log [_] (mapv :request @request-response-log-atom))

  MockModelSpySession
  (-mock-model-spy-log [_] @request-response-log-atom))

(defn create-match-mock-model-session
  [matchers]
  (map->MatchMockModelSession
   {:matchers matchers
    :request-response-log-atom (atom [])}))
