(ns er-cassandra.record.statement-test
  (:require
   [er-cassandra.record.statement :as sut]
   [clojure.test :as t]

   [er-cassandra.util.test :as tu]

   [schema.test :as st]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clj-uuid :as uuid])
  (:import
   [clojure.lang ExceptionInfo]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-session-fixture))

(deftest select-statement-test
  (testing "partition selects"
    (testing "simplest select"
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {}))))

    (testing "compound key"
      (is (=
           {:select :foos :columns :* :where [[:= :foo "foo"] [:= :bar "bar"]]}
           (sut/select-statement
            :foos
            [[:foo] :bar]
            ["foo" "bar"]
            {}))))

    (testing "with columns"
      (is (=
           {:select :foos :columns [:id] :where [[:= :id "foo"]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:columns [:id]}))))

    (testing "with extra where"
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"] [:= :bar "bar"]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:where [[:= :bar "bar"]]})))
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"] [:= :bar "bar"] [:= :baz "baz"]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:where [[:= :bar "bar"]
                     [:= :baz "baz"]]}))))

    (testing "with order-by"
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"]] :order-by [[:foo :asc]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:order-by [[:foo :asc]]})))
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"]] :order-by [[:foo :asc] [:bar :desc]]}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:order-by [[:foo :asc] [:bar :desc]]}))))

    (testing "limit"
      (is (=
           {:select :foos :columns :* :where [[:= :id "foo"]] :limit 5000}
           (sut/select-statement
            :foos
            :id
            "foo"
            {:limit 5000}))))

    (testing "does not permit filtering"
      (is (thrown-with-msg?
           ExceptionInfo #"does not match schema"
           (sut/select-statement
            :foos
            :id
            "foo"
            {:allow-filtering true}))))

    (testing "throws with unknown opt"
      (is (thrown-with-msg?
           ExceptionInfo #"does not match schema"
           (sut/select-statement
            :foos
            :id
            "foo"
            {:blah true})))))

  (testing "table-scan selects"
    (testing "simple table scan"
      (is (= {:select :foos, :columns :*}
             (sut/select-statement :foos {}))))
    (testing "with extra where"
      (is (= {:select :foos, :columns :* :where [[:= :bar "bar"] [:= :baz "baz"]]}
             (sut/select-statement
              :foos
              {:where [[:= :bar "bar"] [:= :baz "baz"]]}))))
    (testing "with limit"
      (is (= {:select :foos, :columns :* :limit 5000}
             (sut/select-statement :foos {:limit 5000}))))
    (testing "does permit filtering"
      (is (= {:select :foos
              :columns :*
              :where [[:= :bar "bar"] [:= :baz "baz"]]
              :allow-filtering true}
             (sut/select-statement
              :foos
              {:where [[:= :bar "bar"] [:= :baz "baz"]]
               :allow-filtering true}))))
    (testing "throws with unknown opt"
      (is (thrown-with-msg?
           ExceptionInfo #"does not match schema"
           {:select :foos :columns :*}
           (sut/select-statement
            :foos
            {:blah true}))))))

(deftest insert-statement-test
  (testing "simple insert"
    (is (= {:insert :foos :values {:id "id" :foo "foo"}}
           (sut/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {}))))
  (testing "collection colls"
    (testing "with plain values"
      (is (= {:insert :foos
              :values {:id "id"
                       :foo [10 20]}}
             (sut/insert-statement
              :foos
              {:id "id"
               :foo [10 20]}
              {})))
      (is (= {:insert :foos
              :values {:id "id"
                       :foo {"bar" 20}}}
             (sut/insert-statement
              :foos
              {:id "id"
               :foo {"bar" 20}}
              {}))))
    (testing "with minimal change diffs"
      (is (= {:insert :foos
              :values {:id "id"
                       :foo [10 20]}}
             (sut/insert-statement
              :foos
              {:id "id"
               :foo {:intersection []
                     :prepended [10]
                     :appended [20]
                     :removed [30 40]}}
              {})))
      (is (= {:insert :foos
              :values {:id "id"
                       :foo [10 20]}}
             (sut/insert-statement
              :foos
              {:id "id"
               :foo {:intersection [10]
                     :appended [20]
                     :removed [200]}}
              {})))))
  (testing "with ttl"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :using [[:ttl 5000]]}
           (sut/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:using {:ttl 5000}}))))
  (testing "with timestamp"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :using [[:timestamp 5000]]}
           (sut/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:using {:timestamp 5000}}))))
  (testing "with if-not-exists"
    (is (= {:insert :foos
            :values {:id "id" :foo "foo"}
            :if-exists false}
           (sut/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:if-not-exists true}))))
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"does not match schema"
           (sut/insert-statement
            :foos
            {:id "id"
             :foo "foo"}
            {:blah true})))))

(deftest update-statement-test
  (testing "simple update"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}
            {}))))
  (testing "compound key, multiple cols"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]
                          [:bar "bar"]]
            :where [[:= :id 100] [:= :id2 200]]}
           (sut/update-statement
            :foos
            [:id :id2]
            {:id 100
             :id2 200
             :foo "foo"
             :bar "bar"}
            {}))))
  (testing "set-columns"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:set-columns [:foo]}))))
  (testing "only-if"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]
                          [:bar "bar"]]
            :where [[:= :id 100]]
            :if [[:= :foo "foo"]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:only-if [[:= :foo "foo"]]}))))

  (testing "if-exists"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]
                          [:bar "bar"]]
            :where [[:= :id 100]]
            :if-exists true}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:if-exists true}))))

  (testing "if-not-exists"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]
                          [:bar "bar"]]
            :where [[:= :id 100]]
            :if-exists false}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"
             :bar "bar"}
            {:if-not-exists true}))))
  (testing "using ttl"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]]
            :where [[:= :id 100]]
            :using [[:ttl 5000]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}
            {:using {:ttl 5000}}))))
  (testing "using timestamp"
    (is (= {:update :foos
            :set-columns [[:foo "foo"]]
            :where [[:= :id 100]]
            :using [[:timestamp 5000]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo "foo"}
            {:using {:timestamp 5000}}))))
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"does not match schema"
                         (sut/update-statement
                          :foos
                          [:id]
                          {:id 100
                           :foo "foo"}
                          {:blah true}))))
  (testing "collection coll diffs"
    (is (= {:update :foos
            :set-columns [[:foo [10 20]]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :foo {:intersection []
                   :appended [10 20]
                   :removed  [1  2]}}
            {})))
    (is (= {:update :foos
            :set-columns [[:bar 1]
                          [:foo #{}]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :bar 1
             :foo {:intersection #{}
                   :removed #{2}}}
            {})))
    (is (= {:update :foos
            :set-columns [[:bar 1]
                          [:foo [+ {"foo" 2}]]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :bar 1
             :foo {:intersection {"baz" 1}
                   :appended {"foo" 2}}}
            {})))
    (is (= {:update :foos
            :set-columns [[:bar 1]
                          [:foo [[0] +]]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :bar 1
             :foo {:intersection [1 2 3]
                   :prepended [0]}}
            {})))
    (is (= {:update :foos
            :set-columns [[:bar 1]
                          [:foo [[0] +]]
                          [:foo [+ [4]]]]
            :where [[:= :id 100]]}
           (sut/update-statement
            :foos
            [:id]
            {:id 100
             :bar 1
             :foo {:intersection [1 2 3]
                   :prepended [0]
                   :appended [4]}}
            {})))))

(deftest delete-statement-test
  (testing "simple delete"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]}
           (sut/delete-statement
            :foos
            :id
            10
            {})))
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :id2 20]]}
           (sut/delete-statement
            :foos
            [:id :id2]
            [10 20]
            {})))
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :id2 20]]}
           (sut/delete-statement
            :foos
            [:id :id2]
            {:id 10 :id2 20}
            {}))))
  (testing "using timestamp"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10]]
            :using [[:timestamp 5000]]}
           (sut/delete-statement
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
           (sut/delete-statement
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
           (sut/delete-statement
            :foos
            :id
            10
            {:if-exists true}))))
  (testing "additional where"
    (is (= {:delete :foos
            :columns :*
            :where [[:= :id 10][:= :foo "foo"][:= :bar "bar"]]}
           (sut/delete-statement
            :foos
            :id
            10
            {:where [[:= :foo "foo"][:= :bar "bar"]]}))))
  (testing "unknown opts"
    (is (thrown-with-msg? ExceptionInfo #"does not match schema"
           (sut/delete-statement
            :foos
            :id
            10
            {:blah true})))))

(deftest prepare-update-statement-test
  (testing "simple prepared statement"
    (is (= {:update :foos
            :set-columns [[:foo :set_foo]]
            :where [[:= :id :where_id]]}
           (sut/prepare-update-statement
            :foos
            :id
            {:id 100
             :foo "foo"}
            {}))))
  (testing "compound key, multiple cols"
    (is (= {:update :foos
            :set-columns [[:foo :set_foo]
                          [:bar :set_bar]]
            :where [[:= :id :where_id]
                    [:= :id2 :where_id2]]}
           (sut/prepare-update-statement
            :foos
            [:id :id2]
            {:id 100
             :id2 200
             :foo "foo"
             :bar "bar"}
            {}))))
  (testing "with options"
    (let [base-cql {:update :foos
                    :set-columns [[:foo :set_foo]
                                  [:bar :set_bar]]
                    :where [[:= :id :where_id]]}
          base-record {:id 100
                       :foo "foo"
                       :bar "bar"}
          base-key [:id]

          prepare-update-statement
          (fn -prepare-update-statement
            ([opt]
             (-prepare-update-statement base-key base-record opt))
            ([record opt]
             (-prepare-update-statement base-key record opt))
            ([key record opt]
             (sut/prepare-update-statement :foos key record opt)))]
      (testing "set-columns"
        (is (= (update base-cql :set-columns (partial remove (fn [[k _]] (= :bar k))))
               (prepare-update-statement {:set-columns [:foo]}))))
      (testing "only-if"
        (is (= (assoc base-cql :if [[:= :foo "foo"]])
               (prepare-update-statement {:only-if [[:= :foo "foo"]]}))))
      (testing "if-exists"
        (is (= (assoc base-cql :if-exists true)
               (prepare-update-statement {:if-exists true}))))
      (testing "if-not-exists"
        (is (= (assoc base-cql :if-exists false)
               (prepare-update-statement {:if-not-exists true}))))
      (testing "using ttl"
        (is (= (assoc base-cql :using [[:ttl :using_ttl]])
               (prepare-update-statement {:using {:ttl 5000}}))))
      (testing "using timestamp"
        (is (= (assoc base-cql :using [[:timestamp :using_timestamp]])
               (prepare-update-statement {:using {:timestamp 5000}}))))
      (testing "unknown opts"
        (is (thrown-with-msg?
             ExceptionInfo
             #"does not match schema"
             (prepare-update-statement {:blah true}))))))
  (testing "collection coll diffs"
    (let [base-update {:update :foos
                       :where [[:= :id :where_id]]}
          append-to-col-cql (merge
                             base-update
                             {:set-columns [[:foo [+ :set_append_foo]]]})
          prepend-to-col-cql (merge
                              base-update
                              {:set-columns [[:foo [:set_prepend_foo +]]]})
          add-to-col-cql (merge
                          base-update
                          {:set-columns [[:foo [:set_prepend_foo +]]
                                         [:foo [+ :set_append_foo]]]})
          set-col-cql (merge
                       base-update
                       {:set-columns [[:foo :set_foo]]})
          base-record {:id 100}
          prepare-update-statement (fn [record]
                                     (sut/prepare-update-statement
                                      :foos
                                      [:id]
                                      (merge base-record record)
                                      {}))]
      (is (= append-to-col-cql
             (prepare-update-statement
              {:foo {:intersection {}
                     :appended {:a 20}}})))
      (is (= prepend-to-col-cql
             (prepare-update-statement
              {:foo {:intersection [1 2 3]
                     :prepended [0]}})))
      (is (= add-to-col-cql
             (prepare-update-statement
              {:foo {:intersection [1 2 3]
                     :prepended [0]
                     :appended [4]}})))
      (is (= set-col-cql
             (prepare-update-statement
              {:foo {:intersection #{}
                     :removed #{10}}})))
      (is (= set-col-cql
             (prepare-update-statement
              {:foo {:intersection [0]
                     :appended [10 20]
                     :removed  [1  2]}}))))))

(deftest prepare-update-values-test
  (testing "simple record"
    (is (= {:set_foo "foo"
            :set_bar "bar"
            :where_id 100}
           (sut/prepare-update-values
            :foos
            :id
            {:id 100
             :foo "foo"
             :bar "bar"}
            {}))))
  (testing "with options"
    (let [base-values {:set_foo "foo"
                       :set_bar "bar"
                       :where_id 100}
          base-record {:id 100
                       :foo "foo"
                       :bar "bar"}

          prepare-update-values
          (fn [opts]
            (sut/prepare-update-values
             :foos
             :id
             base-record
             opts))]
      (testing "set-columns"
        (is (= (dissoc base-values :set_bar)
               (prepare-update-values
                {:set-columns [:foo]}))))
      (testing "using ttl"
        (is (= (assoc base-values :using_ttl 500)
               (prepare-update-values
                {:using {:ttl 500}}))))
      (testing "using timestamp"
        (is (= (assoc base-values :using_timestamp 5000)
               (prepare-update-values
                {:using {:timestamp 5000}}))))))
  (testing "collection values"
    (let [base-record {:id 100}
          base-values {:where_id 100}
          prepare-update-values (fn [record]
                                     (sut/prepare-update-values
                                      :foos
                                      [:id]
                                      (merge base-record record)
                                      {}))]

      (is (= (assoc base-values :set_append_foo {:a 20})
             (prepare-update-values
              {:foo {:intersection {}
                     :appended {:a 20}}})))
      (is (= (assoc base-values :set_prepend_foo [0])
             (prepare-update-values
              {:foo {:intersection [1 2 3]
                     :prepended [0]}})))
      (is (= (assoc
              base-values
              :set_prepend_foo [0]
              :set_append_foo [4])
             (prepare-update-values
              {:foo {:intersection [1 2 3]
                     :prepended [0]
                     :appended [4]}})))
      (is (= (assoc base-values :set_foo #{})
             (prepare-update-values
              {:foo {:intersection #{}
                     :removed #{10}}})))
      (is (= (assoc base-values :set_foo [0 10 20])
             (prepare-update-values
              {:foo {:intersection [0]
                     :appended [10 20]
                     :removed  [1  2]}}))))))

(deftest prepare-record-values-test
  (testing "simple record"
    (is (= {:set_id 100
            :set_foo "foo"
            :set_bar "bar"}
           (sut/prepare-record-values
            :set
            {:id 100
             :foo "foo"
             :bar "bar"}))))
  (testing "collection values"
    (testing "using plain values"
      (is (= {:set_id 100
              :set_foo [10 20]}
             (sut/prepare-record-values
              :set
              {:id 100
               :foo [10 20]}))))
    (testing "using collection coll diffs"
      (is (= {:set_id 100
              :set_foo [10 20]}
             (sut/prepare-record-values
              :set
              {:id 100
               :foo {:intersection []
                     :appended [10 20]
                     :removed  [30 40]}})))
      (is (= {:set_id 100
              :set_foo [10 20]}
             (sut/prepare-record-values
              :set
              {:id 100
               :foo {:intersection [10]
                     :appended [20]
                     :removed  [200]}}))))))
