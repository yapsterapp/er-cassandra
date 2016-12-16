(ns er-cassandra.record-test
  (:require
   [er-cassandra.record :as r]
   [clojure.test :refer [deftest is testing]])
  (:import
   [clojure.lang ExceptionInfo]))

(deftest select-statement-test
  (testing "simplest select"
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"]]}
         (r/select-statement
          :foos
          :id
          "foo"))))

  (testing "compound key"
    (is (=
         {:select :foos :columns :* :where [[:= :foo "foo"] [:= :bar "bar"]]}
         (r/select-statement
          :foos
          [[:foo] :bar]
          ["foo" "bar"]))))

  (testing "with columns"
    (is (=
         {:select :foos :columns [:id] :where [[:= :id "foo"]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:columns [:id]}))))

  (testing "with extra where"
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"] [:= :bar "bar"]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:where [:= :bar "bar"]})))
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"] [:= :bar "bar"] [:= :baz "baz"]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:where [[:= :bar "bar"]
                   [:= :baz "baz"]]}))))

  (testing "with order-by"
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"]] :order-by [[:foo :asc]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:order-by [[:foo :asc]]})))
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"]] :order-by [[:foo :asc] [:bar :desc]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:order-by [[:foo :asc] [:bar :desc]]}))))

  (testing "limit"
    (is (=
         {:select :foos :columns :* :where [[:= :id "foo"]] :limit 5000}
         (r/select-statement
          :foos
          :id
          "foo"
          {:limit 5000}))))

  (testing "throws with unknown opt"
    (is (thrown-with-msg? ExceptionInfo #"unknown opts"
         {:select :foos :columns :* :where [[:= :id "foo"]]}
         (r/select-statement
          :foos
          :id
          "foo"
          {:blah true})))))

(deftest insert-statement-test
  (testing "simple insert"
    (is (= {:insert :foos :values {:id "id" :foo "foo"}}
           (r/insert-statement
            :foos
            {:id "id"
             :foo "foo"}))))
  (testing "with ttl"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :using [[:ttl 5000]]}
           (r/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:using {:ttl 5000}}))))
  (testing "with timestamp"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :using [[:timestamp 5000]]}
           (r/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:using {:timestamp 5000}}))))
  (testing "with if-not-exists"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :if-exists false}
           (r/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:if-not-exists true}))))

)
