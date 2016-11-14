(ns er-cassandra.model.mock.match-mock-model-session
  (:require
   [plumbing.core :refer :all]
   [er-cassandra.model.model-session
    :refer [ModelSession ModelSpySession]]
   [er-cassandra.model.mock.mock-model-session :as mms
    :refer [MockModelSession MockModelSpySession Matcher
            -match -finish]]))

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
                   :key key
                   :record-or-key-value record-or-key-value
                   :opts opts}
          response (match matchers request)]
      (swap! request-response-log-atom conj {:request request :response response})
      response))

  (-close [_])

  MockModelSession
  (-check [_]
    (->> matchers
         (map -finish)
         (filter identity)
         doall)
    true)


  ModelSpySession
  (-model-spy-log [_] (mapv :request @request-response-log-atom))

  MockModelSpySession
  (-mock-model-spy-log [_] @request-response-log-atom))

(defn create
  [matchers]
  (map->MatchMockModelSession
   {:matchers matchers
    :request-response-log-atom (atom [])}))

(defn with-match-mock-model-session*
  [match-mock-model-session body-fn]
  (assert (instance? MatchMockModelSession match-mock-model-session))
  (try
    (body-fn match-mock-model-session)
    match-mock-model-session
    (finally
      (mms/-check match-mock-model-session))))

(defmacro with-match-mock-model-session
  [binding & body]
  (assert (vector? binding))
  (assert (= 2 (count binding)))
  (let [[sym sess] binding]
    (assert (symbol? sym))
    (assert (not (nil? sess)))

    `(with-match-mock-model-session*
       ~sess
       (fn [~sym]
         ~@body))))
