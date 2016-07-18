(ns er-cassandra.model.mock.mock-model-session-matchers
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.core.match :refer [match]]
   [er-cassandra.model.mock.mock-model-session
    :refer [Matcher -match]]))

(defn any
  [response]
  (reify
    Matcher
    (-match [_ request]
      response)
    (-finish [_] nil)))

(defn times
  [matcher n]
  (let [counter (atom 0)]
    (reify
      Matcher
      (-match [_ request]
        (if-let [m (-match matcher request)]
          (do
            (swap! counter inc)
            (if (<= @counter n)
              m
              (throw (ex-info
                      (str "seen " @counter " times, but required " n " times")
                      {:request request
                       :counter @counter
                       :n n
                       :matcher matcher}))))
          nil))
      (-finish [_]
        (if (< @counter n)
          (throw (ex-info
                  (str "seen " @counter " times, but required " n " times")
                  {:counter @counter
                   :n n
                   :matcher matcher})))))))

(defn once
  [matcher]
  (times matcher 1))

(defn never
  [matcher]
  (times matcher 0))

(defn select
  ([model key record-or-key-value response]
   (select model key record-or-key-value {} response))
  ([model key record-or-key-value opts response]
   (reify
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
     (-finish [_] nil))))

(defn upsert
  ([model record response]
   (upsert model record {} response))
  ([model record opts response]
   (reify
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
     (-finish [_] nil))))

(defn delete
  ([model key record-or-key-value response]
   (delete model key record-or-key-value {} response))
  ([model key record-or-key-value opts response]
   (reify
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
     (-finish [_] nil))))
