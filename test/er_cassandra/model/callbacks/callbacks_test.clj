(ns er-cassandra.model.callbacks.callbacks-test
  (:require
   [clojure.test :as t :refer [deftest is are use-fixtures testing]]
   [schema.test :as st]
   [er-cassandra.model.callbacks.protocol :as cb]
   [er-cassandra.model.callbacks :as sut]))

(use-fixtures :once st/validate-schemas)

(deftest create-updated-at-callback-test
  (testing "does nothing if no changes"
    (let [cb (sut/create-updated-at-callback)
          rec {:foo 10 :bar "bar"}]
      (is (= rec
             (cb/-before-save cb nil rec rec {})))))
  (testing "adds col to new record"
    (let [cb (sut/create-updated-at-callback)
          rec {:foo 10 :bar "bar"}
          r (cb/-before-save cb nil nil rec {})]
      (contains? r :updated_at)
      (is (= rec (dissoc r :updated_at)))))
  (testing "adds col to changed record"
    (let [cb (sut/create-updated-at-callback)
          rec {:foo 10 :bar "bar"}
          r (cb/-before-save cb nil rec (assoc rec :bar "baz") {})]
      (contains? r :updated_at)
      (is (= (assoc rec :bar "baz")
             (dissoc r :updated_at)))))
  (testing "adds custom col"
    (let [cb (sut/create-updated-at-callback :also_updated_at)
          rec {:foo 10 :bar "bar"}
          r (cb/-before-save cb nil rec (assoc rec :bar "baz") {})]
      (contains? r :also_updated_at)
      (is (= (assoc rec :bar "baz")
             (dissoc r :also_updated_at))))))
