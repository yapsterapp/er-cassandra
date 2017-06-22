(ns er-cassandra.model.types-test
  (:require
   [clojure.test :as test :refer [deftest is are use-fixtures]]
   [schema.test :as st]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.types :refer :all]))

(use-fixtures :once st/validate-schemas)

(deftest test-satisfies-primary-key?
  (is (thrown? AssertionError (satisfies-primary-key? :foo [:foo])))
  (is (thrown? AssertionError (satisfies-primary-key? [:foo] :foo)))

  (is (satisfies-primary-key? [:foo] [:foo]))

  (is (not (satisfies-primary-key? [:foo] [])))
  (is (not (satisfies-primary-key? [:foo] [:foo :bar])))

  (is (satisfies-primary-key? [:foo :bar] [:foo :bar]))
  (is (satisfies-primary-key? [[:foo] :bar] [:foo :bar])))

(deftest test-satifsies-partition-key?
  (is (thrown? AssertionError (satisfies-partition-key? :foo [:foo])))
  (is (thrown? AssertionError (satisfies-partition-key? [:foo] :foo)))

  (is (satisfies-partition-key? [:foo] [:foo]))
  (is (not (satisfies-partition-key? [:foo] [:foo :bar])))
  (is (satisfies-partition-key? [[:foo] :bar] [:foo]))
  (is (satisfies-partition-key? [[:foo :bar] :baz] [:foo :bar])))

(deftest test-satisfies-cluster-key?
  (is (thrown? AssertionError (satisfies-cluster-key? :foo [:foo])))
  (is (thrown? AssertionError (satisfies-cluster-key? [:foo] :foo)))

  (is (satisfies-cluster-key? [:foo] [:foo]))
  (is (not (satisfies-cluster-key? [:foo] [:foo :bar])))
  (is (satisfies-cluster-key? [:foo :bar] [:foo]))
  (is (satisfies-cluster-key? [:foo :bar] [:foo :bar]))
  (is (not (satisfies-cluster-key? [:foo :bar] [:foo :baz])))
  (is (satisfies-cluster-key? [:foo :bar :baz] [:foo :bar]))

  (is (not (satisfies-cluster-key? [[:foo :bar] :baz] [:foo])))
  (is (satisfies-cluster-key? [[:foo :bar] :baz] [:foo :bar]))
  (is (satisfies-cluster-key? [[:foo :bar] :baz] [:foo :bar :baz])))

(deftest test-mutable-secondary-tables
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_baz
                                :key [:baz]
                                :view? true}]})]
    (is (= (->> m :secondary-tables (take 1))
           (t/mutable-secondary-tables m)))))

(deftest test-mutable-lookup-tables
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :view? true}]})]
    (is (= (->> m :lookup-tables (take 1))
           (t/mutable-lookup-tables m)))))
