(ns er-cassandra.model.callbacks.unsafe-created-at-callback-test
  (:require
   [er-cassandra.model.callbacks.unsafe-created-at-callback :as cb]
   [clj-time.core :as time]
   [clojure.test :as t :refer [deftest is are use-fixtures testing]])
  (:import
   [java.util Date]))

(deftest unsafe-created-at-callback-test
  (testing ":created_at is set"
    (let [cb (cb/unsafe-created-at-callback)
          r (cb {})]
      (is (some? (:created_at r)))
      (is (= Date
             (.getClass (:created_at r))))))

  (testing ":created_at is not overwritten"
    (let [cb (cb/unsafe-created-at-callback)
          t (-> 10 time/seconds time/ago .toDate)
          r (cb {:created_at t})]
      (is (= t (:created_at r)))))

  (testing "custom col is set"
    (let [cb (cb/unsafe-created-at-callback :foo)
          r (cb {})]
      (is (some? (:foo r)))
      (is (= Date
             (.getClass (:foo r)))))))
