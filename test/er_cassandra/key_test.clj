(ns er-cassandra.key-test
  (:require [clojure.test :as test :refer [deftest is are testing]]
            [er-cassandra.key :as k]))

(deftest partition-key-test
  (is (= [:foo] (k/partition-key :foo)))
  (is (= [:foo] (k/partition-key [:foo])))
  (is (= [:foo] (k/partition-key [:foo :bar])))
  (is (= [:foo :bar] (k/partition-key [[:foo :bar] :baz]))))

(deftest cluster-key-test
  (is (= nil (k/cluster-key :foo)))
  (is (= [:bar] (k/cluster-key [:foo :bar])))
  (is (= [:bar :baz] (k/cluster-key [:foo :bar :baz])))
  (is (= [:baz :boo] (k/cluster-key [[:foo :bar] :baz :boo]))))

(deftest extract-key-value-test
  (testing "single component key"
    (is (= [10] (k/extract-key-value :id 10 {})))
    (is (= [10] (k/extract-key-value [:id] 10 {})))
    (is (= [10] (k/extract-key-value :id [10] {})))
    (is (= [10] (k/extract-key-value [:id] [10] {})))
    (is (= [10] (k/extract-key-value :id {:id 10} {})))
    (is (= [10] (k/extract-key-value [:id] {:id 10} {})))
    (is (= [10] (k/extract-key-value :id {} {:key-value 10})))
    (is (= [10] (k/extract-key-value [:id] {} {:key-value 10})))
    (is (= [10] (k/extract-key-value :id {} {:key-value [10]})))
    (is (= [10] (k/extract-key-value [:id] {} {:key-value [10]})))
    (is (= [10] (k/extract-key-value [:id] {:id 1} {:key-value [10]}))))
  (testing "multi component key"
    (is (= [10 20] (k/extract-key-value [:foo :bar] [10 20] {})))
    (is (= [10 20] (k/extract-key-value [:foo :bar] {:foo 10 :bar 20} {})))
    (is (= [10 20] (k/extract-key-value
                    [:foo :bar] {} {:key-value [10 20]})))
    (is (= [10 20] (k/extract-key-value
                    [:foo :bar] {:foo 100 :bar 200} {:key-value [10 20]})))
    (is (= [10 20] (k/extract-key-value
                    [:foo :bar] {:foo 10 :bar 200} {:key-value [nil 20]})))
    (is (= [10 20] (k/extract-key-value
                    [:foo :bar] {:foo 10 :bar 200} {:key-value [nil 20]})))))

(deftest remove-key-components-test
  (is (= nil (k/remove-key-components :id 10 :id)))
  (is (= nil (k/remove-key-components [:id] [10] [:id])))
  (is (= [[:foo] [10]] (k/remove-key-components [:foo :bar] [10 20] [:bar])))
  (is (= [[:bar :boo] [20 40]]
         (k/remove-key-components [:foo :bar :baz :boo] [10 20 30 40] [:foo :baz]))))

(deftest key-equality-clause-test
  (is (= [[:= :id 10]] (k/key-equality-clause :id 10)))
  (is (= [[:= :id 10]] (k/key-equality-clause [:id] [10])))
  (is (= [[:= :foo 10] [:= :bar 20]]
         (k/key-equality-clause [:foo :bar] [10 20])))
  (is (= [[:= :foo 10] [:in :bar ["one" "two"]]]
         (k/key-equality-clause [:foo :bar] [10 ["one" "two"]]))))

(deftest extract-key-equality-clause-test
  (is (= [[:= :id 10]] (k/extract-key-equality-clause :id 10)))
  (is (= [[:= :id 10]] (k/extract-key-equality-clause [:id] [10])))
  (is (= [[:= :id 10]] (k/extract-key-equality-clause [:id] {:id 10})))
  (is (= [[:= :foo 10] [:= :bar 20]]
         (k/extract-key-equality-clause [:foo :bar] {:foo 10 :bar 20})))
  (is (= [[:= :foo 10] [:= :bar 20]]
         (k/extract-key-equality-clause
          [:foo :bar] {:foo 10 :bar 200} {:key-value [nil 20]}))))

(deftest extract-collection-key-components-test
  (testing "extract list values"
    (is (= [1 2 3]
           (k/extract-collection-key-components
            {:id :list} :id [1 2 3 nil] {})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"col is not a list"
         (k/extract-collection-key-components
          {:id :list} :id #{1 2 3 nil} {}))))

  (testing "extract set values"
    (is (= #{1 2 3}
           (k/extract-collection-key-components
            {:id :set} :id #{1 2 3 nil} {})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"col is not a set"
         (k/extract-collection-key-components
          {:id :set} :id [1 2 3 nil] {}))))

  (testing "extract map values"
    (is (= [:foo :bar :baz]
           (k/extract-collection-key-components
            {:id :map} :id {:foo 10 :bar 20 :baz 30 nil 40} {})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"col is not a map"
         (k/extract-collection-key-components
          {:id :map} :id [1 2 3 nil] {})))))

(deftest extract-key-value-collection-test
  (testing "extract key collections"
    (is (= (set [[:fa :ba]])
           (set (k/extract-key-value-collection
                 [:foo :bar]
                 {:foo [:fa nil] :bar [nil :ba]}
                 {:foo :list :bar :list}))))
    (is (= (set [[:fa :ba] [:fa :bb]])
           (set (k/extract-key-value-collection
                 [:foo :bar]
                 {:foo [:fa nil] :bar #{:ba :bb nil}}
                 {:foo :list :bar :set}))))
    (is (= (set [[:fa :ba] [:fa :bb] [:fb :ba] [:fb :bb]])
           (set (k/extract-key-value-collection
                 [:foo :bar]
                 {:foo [:fa :fb] :bar [:ba :bb]}
                 {:foo :list :bar :list}))))))

(deftest has-key?-test
  (are [k expected-val] (= (k/has-key? k {:x 1 :y 2 :z 3})
                           expected-val)
    [:x]      true
    [[:x] :z] true
    [[:x] :A] false
    [[:x :y]] true))
