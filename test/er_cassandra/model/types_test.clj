(ns er-cassandra.model.types-test
  (:require
   [clojure.test :as test :refer [deftest is are use-fixtures testing]]
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

(deftest all-key-cols-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :unique-key-tables [{:name :foos_by_email :key [:email]}]
            :secondary-tables [{:name :foos_by_bar :key [:bar]}]
            :lookup-tables [{:name :foos_by_baz
                             :key [:baz]
                             :view? true}]})]
    (is (= #{:id :bar :baz :email}
           (set (t/all-key-cols m))))))

(deftest all-maintained-foreign-key-cols-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :unique-key-tables [{:name :foos_by_email :key [:email]}]
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_stuff
                                :key [:stuff]
                                :view? true}]
            :lookup-tables [{:name :foos_by_baz
                             :key [:baz]}
                            {:name :foos_by_thing
                             :key [:thing]
                             :view? true}]})]
    (is (= #{:bar :baz :email}
           (set
            (t/all-maintained-foreign-key-cols m))))))

(defn foos-all-table-types-entity
  []
  (t/create-entity
   {:primary-table {:name :foos :key [:id]}
    :unique-key-tables [{:name :foos_by_email :key [:email]}]
    :secondary-tables [{:name :foos_by_bar
                        :key [[:bar] :id]}
                       {:name :foos_by_stuff
                        :key [[:stuff] :id]
                        :view? true}]
    :lookup-tables [{:name :foos_by_baz
                     :key [[:baz] :id]}
                    {:name :foos_by_thing
                     :key [[:thing] :id]
                     :view? true}]}))

(deftest all-entity-tables-test
  (let [{pt :primary-table
         ukts :unique-key-tables
         sts :secondary-tables
         lts :lookup-tables
         :as m} (foos-all-table-types-entity)]
    (is (= (->> (apply concat [[pt] ukts sts lts])
                (map :name)
                set)
           (->>
            (t/all-entity-tables m)
            (map :name)
            set)))))

(deftest contains-key-cols-for-table?-test
  (let [{pt :primary-table
         [foos-by-email
          :as ukts] :unique-key-tables
         [foos-by-bar
          :as sts] :secondary-tables
         lts :lookup-tables
         :as m} (foos-all-table-types-entity)]
    (testing "contains single-col key cols"
      (is
       (= true
          (t/contains-key-cols-for-table?
           m {:email "foo@bar.com"} foos-by-email)))
      (is
       (= true
          (t/contains-key-cols-for-table?
           m {:email nil} foos-by-email)))
      (is
       (= false
          (t/contains-key-cols-for-table?
           m {} foos-by-email))))
    (testing "contains multi-col key"
      (is
       (= true
          (t/contains-key-cols-for-table?
           m {:id 10 :bar "blah"} foos-by-bar)))
      (is
       (= true
          (t/contains-key-cols-for-table?
           m {:id 10 :bar nil} foos-by-bar)))
      (is
       (= false
          (t/contains-key-cols-for-table?
           m {} foos-by-bar))))))
