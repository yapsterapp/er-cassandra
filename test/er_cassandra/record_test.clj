(ns er-cassandra.record-test
  (:require
   [er-cassandra.util.test :as tu]
   [er-cassandra.session.alia :as alia-session]
   [er-cassandra.record :as r]
   [clojure.test :refer [deftest is testing use-fixtures]])
  (:import
   [clojure.lang ExceptionInfo]))

(use-fixtures :each (tu/with-session-fixture))

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
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"unknown opts"
           (r/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:blah true})))))

(deftest update-statement-test
  (testing "simple update"
    (is (= {:update :foos
            :set-columns {:foo "foo"}
            :where [[:= :id 100]]}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}))))
  (testing "compound key, multiple cols"
    (is (= {:update :foos
            :set-columns {:foo "foo" :bar "bar"}
            :where [[:= :id 100] [:= :id2 200]]}
           (r/update-statement
            :foos
            [:id :id2]
            {:id 100
             :id2 200
             :foo "foo"
             :bar "bar"}))))
  (testing "set-columns"
    (is (= {:update :foos
            :set-columns {:foo "foo"}
            :where [[:= :id 100]]}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:set-columns [:foo]}))))
  (testing "only-if"
    (is (= {:update :foos
            :set-columns {:foo "foo" :bar "bar"}
            :where [[:= :id 100]]
            :if [[:= :foo "foo"]]}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:only-if [[:= :foo "foo"]]}))))

  (testing "if-exists"
    (is (= {:update :foos
            :set-columns {:foo "foo" :bar "bar"}
            :where [[:= :id 100]]
            :if-exists true}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:if-exists true}))))

  (testing "if-not-exists"
    (is (= {:update :foos
            :set-columns {:foo "foo" :bar "bar"}
            :where [[:= :id 100]]
            :if-exists false}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:if-not-exists true}))))
  (testing "using ttl"
    (is (= {:update :foos
            :set-columns {:foo "foo"}
            :where [[:= :id 100]]
            :using [[:ttl 5000]]}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}
            {:using {:ttl 5000}}))))
  (testing "using timestamp"
    (is (= {:update :foos
            :set-columns {:foo "foo"}
            :where [[:= :id 100]]
            :using [[:timestamp 5000]]}
           (r/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}
            {:using {:timestamp 5000}}))))
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"unknown opts"
                         (r/update-statement
                          :foos
                          [:id]
                          {:id 100
                           :foo "foo"}
                          {:blah true})))))

(deftest delete-statement-test
  (testing "simple delete"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]}
           (r/delete-statement
            :foos
            :id
            10)))
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :id2 20]]}
           (r/delete-statement
            :foos
            [:id :id2]
            [10 20])))
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :id2 20]]}
           (r/delete-statement
            :foos
            [:id :id2]
            {:id 10 :id2 20}))))
  (testing "using timestamp"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]
            :using [[:timestamp 5000]]}
           (r/delete-statement
            :foos
            :id
            10
            {:using {:timestamp 5000}}))))
  (testing "only-if"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]
            :if [[:= :foo "foo"]]
            }
           (r/delete-statement
            :foos
            :id
            10
            {:only-if [[:= :foo "foo"]]}))))
  (testing "if-exists"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]
            :if-exists true
            }
           (r/delete-statement
            :foos
            :id
            10
            {:if-exists true}))))
  (testing "additional where"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :foo "foo"][:= :bar "bar"]]}
           (r/delete-statement
            :foos
            :id
            10
            {:where [[:= :foo "foo"][:= :bar "bar"]]}))))
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"unknown opts"
           (r/delete-statement
            :foos
            :id
            10
            {:blah true})))))

(deftest select-test
  (testing "simple select"
    @(r/select tu/*session* :foos :id "foo")
    ))
