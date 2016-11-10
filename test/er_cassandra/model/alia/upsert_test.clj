(ns er-cassandra.model.alia.upsert-test
  (:require
   [clojure.test :as test :refer [deftest is are testing]]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.upsert :as u]))

(deftest stale-lookup-key-values-test
  (let [m (t/create-model
           {:primary-table {:name :foos :key [:id]}
            :lookup-key-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           (u/stale-lookup-key-values
            m
            {:id :a :bar :b}
            {:id :a}
            (-> m :lookup-key-tables first)))))

    (testing "correctly identifies a stale singular lookup key values"
      (is (= [[:b]]
             (u/stale-lookup-key-values
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :lookup-key-tables first)))))

    (testing "correctly identifiers stale collection lookup key values"
      (is (= #{[:b] [:d]}
             (set
              (u/stale-lookup-key-values
               m
               {:id :a :baz #{:b :c :d}}
               {:id :a :baz #{:c}}
               (-> m :lookup-key-tables second))))))))

(deftest stale-secondary-key-value-test
  (let [m (t/create-model
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}]})
        st (-> m :secondary-tables first)]

    (testing "ignores secondary keys when missing from new-record"
      (is (= nil
             (u/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a}
              (-> m :secondary-tables first)))))

    (testing "correctly identifies a stale singular secondary key value"
      (is (= [:b]
             (u/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :secondary-tables first)))))))

(deftest with-columns-option-for-lookups
  (let [m (t/create-model
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_baz
                                :key [:baz]}]
            :lookup-key-tables [{:name :foos_by_x
                                 :key [:x]
                                 :with-columns [:c1 :c2]}]})
        record {:id 1 :bar "bar1" :baz "baz1" :c1 :C1 :c2 :C2 :c3 :C3 :x "x-key"}
        [t lrecord] (first (u/lookup-record-seq m record))]
    (are [x y] (= x y)
      1      (count (u/lookup-record-seq m record))
      :C1    (:c1 lrecord)
      :C2    (:c2 lrecord)
      false  (contains? lrecord :c3))))
