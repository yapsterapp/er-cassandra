(ns er-cassandra.model.mock.matchers
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.core.match :refer [match]]
   [er-cassandra.model.mock.mock-model-session
    :refer [Matcher -match]]))

(defrecord AnyMatcher [response]
  Matcher
  (-match [_ request]
    response)
  (-finish [_] nil))

(defn any
  [response]
  (map->AnyMatcher {:response response}))

(defrecord TimesMatcher [matcher n counter]
  Matcher
  (-match [_ request]
    (if-let [m (and (< @counter n)
                    (-match matcher request))]
      (do
        (swap! counter inc)
        m)
      nil))
  (-finish [_]
    (if (not= @counter n)
      (throw (ex-info
              (str "seen " @counter " times, but required " n " times")
              {:counter @counter
               :n n
               :matcher matcher})))))

(defn times
  [matcher n]
  (let [counter (atom 0)]
    (map->TimesMatcher {:matcher matcher :n n :counter counter})))

(defn once
  [matcher]
  (times matcher 1))

(defrecord NeverMatcher [matcher]
  Matcher
  (-match [_ request]
    (when (-match matcher request)
      (throw
       (ex-info
        "should never have matched"
        {:matcher matcher
         :request request}))))
  (-finish [_] nil))

(defn never
  [matcher]
  (map->NeverMatcher {:matcher matcher}))

(defrecord SelectMatcher [model key record-or-key-value opts response]
  Matcher
  (-match [_ request]
    (match [(merge {:opts {}} request)]
      [{:action :select
        :model rm
        :key rk
        :record-or-key-value rrkv
        :opts ro}] (when (and (= rm model)
                              (= rk key)
                              (= rrkv record-or-key-value)
                              (= ro opts))
                     response)
      :else nil))
  (-finish [_] nil))

(defn select
  ([model key record-or-key-value response]
   (select model key record-or-key-value {} response))
  ([model key record-or-key-value opts response]
   (map->SelectMatcher {:model model
                        :key key
                        :record-or-key-value record-or-key-value
                        :opts opts
                        :response response})))

(defn select-one
  ([model key record-or-key-value response]
   (select-one model key record-or-key-value {} response))
  ([model key record-or-key-value opts response]
   (select model key record-or-key-value (merge {:limit 1} opts) response)))

(defrecord UpsertMatcher [model record opts response]
  Matcher
  (-match [_ request]
    (match [(merge {:opts {}} request)]
      [{:action :upsert
        :model rm
        :record rr
        :opts ro}] (when (and (= rm model)
                              (= rr record)
                              (= ro opts))
                     response)
      :else nil))
  (-finish [_] nil))

(defn upsert
  ([model record response]
   (upsert model record {} response))
  ([model record opts response]
   (map->UpsertMatcher {:model model
                        :record record
                        :opts opts
                        :response response})))

(defrecord DeleteMatcher [model key record-or-key-value opts response]
  Matcher
  (-match [_ request]
    (match [(merge {:opts {}} request)]
      [{:action :delete
        :model rm
        :key rk
        :record-or-key-value rrkv
        :opts ro}] (when (and (= rm model)
                              (= rk key)
                              (= rrkv record-or-key-value)
                              (= ro opts))
                     response)
      :else nil))
  (-finish [_] nil))

(defn delete
  ([model key record-or-key-value response]
   (delete model key record-or-key-value {} response))
  ([model key record-or-key-value opts response]
   (map->DeleteMatcher {:model model
                        :key key
                        :record-or-key-value record-or-key-value
                        :opts opts
                        :response response})))
