(ns er-cassandra.model.types-test
  (:require
   [clojure.test :as test :refer [deftest is are]]
   [er-cassandra.model.types :refer :all]))

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
