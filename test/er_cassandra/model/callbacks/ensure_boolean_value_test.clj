(ns er-cassandra.model.callbacks.ensure-boolean-value-test
  (:require
   [clojure.test :refer [deftest is are testing]]
   [er-cassandra.model.callbacks.ensure-boolean-value :as sut]))

(deftest ensure-boolean-value-callback-test
  (let [cb (sut/ensure-boolean-value-callback :flag)]
    (testing "returns original record if col not present"
      (let [record {:id 1}]
        (is (= record
               (cb record)))))
    (testing "replaces nil with false"
      (let [record {:id 1
                    :flag nil}]
        (is (false? (-> record cb :flag)))))
    (testing "returns bool untouched"
      (let [record {:id 1
                    :flag true}]
        (is (true? (-> record cb :flag)))))
    (testing "replaces non-bool with bool"
      (let [record {:id 1
                    :flag 1}]
        (is (true? (-> record cb :flag)))))))
