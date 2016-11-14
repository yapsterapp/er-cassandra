(ns er-cassandra.model.alia.unique-key-test
  (:require
   [clojure.test :as test :refer [deftest is are testing]]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.unique-key :as uk]))

(deftest stale-unique-key-values-test
  (let [m (t/create-model
           {:primary-table {:name :foos :key [:id]}
            :unique-key-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores unique keys when missing from new-record"
      (is (empty?
           (uk/stale-unique-key-values
            m
            {:id :a :bar :b}
            {:id :a}
            (-> m :unique-key-tables first)))))

    (testing "correctly identifies a stale singular unique key values"
      (is (= [[:b]]
             (uk/stale-unique-key-values
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :unique-key-tables first)))))

    (testing "correctly identifiers stale collection unique key values"
      (is (= #{[:b] [:d]}
             (set
              (uk/stale-unique-key-values
               m
               {:id :a :baz #{:b :c :d}}
               {:id :a :baz #{:c}}
               (-> m :unique-key-tables second))))))))
