(ns er-cassandra.model.types-test
  (:require
   [clojure.test :as test :refer [deftest is are use-fixtures testing]]
   [clojure.string :as string]
   [manifold.deferred :as deferred]
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


(deftest test-run-callbacks
  (let [s nil
        m (t/create-entity
           {:primary-table {:name :foos :key [:id]}})
        update-callbacks (fn [m cbs]
                           (assoc-in m [:callbacks :before-save] cbs))
        r {:id 1 :name "bar"}]
      (let [m (update-callbacks m [identity])]
    (testing "identity"
        (is (= r @(run-callbacks s m :before-save r)))))
    (testing "callback sequence"
      (let [m (update-callbacks m [#(assoc % :name "baz")
                                   #(update % :name string/upper-case)])
            exp {:id 1 :name "BAZ"}]
        (is (= exp @(run-callbacks s m :before-save r)))))
    (testing "callback deferred"
      (let [m (update-callbacks m [(reify ICallback
                                     (run-callback [_ session entity record]
                                       (deferred/success-deferred
                                         (update record :name string/upper-case))))])
            exp {:id 1 :name "BAR"}]
        (is (= exp @(run-callbacks s m :before-save r)))))
    (testing "callback error")))
