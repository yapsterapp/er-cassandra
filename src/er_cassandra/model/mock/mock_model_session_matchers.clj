(ns er-cassandra.model.mock.mock-model-session-matchers
  (:require
   [er-cassandra.model.mock.mock-model-session
    :refer [Matcher -match]]))

(defn any
  [response]
  (reify
    Matcher
    (-match [_ request]
      response)
    (-failure [_] nil)))

(defn times
  [matcher n]
  (let [counter (atom 0)]
    (reify
      Matcher
      (-match [_ request]
        (if (< @counter n)
          (if-let [m (-match matcher request)]
            (do
              (swap! counter inc)
              m)
            nil)
          nil))
      (-failure [_] nil))))

(defn once
  [matcher]
  (times matcher 1))

(defn never
  [matcher]
  )
