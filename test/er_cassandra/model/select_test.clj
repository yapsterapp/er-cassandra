(ns er-cassandra.model.select-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.model :as m]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.model-session :as ms]))


(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_model_select_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_model_select_test :key [:id]}}))

(deftest select-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo (ms/create-entity-instance {:id id :nick "foo"})

        _ (insert-record :simple_model_select_test record-foo)]

    (testing "selecting record-foo"
      (let [[r e] @(m/select
                    tu/*model-session*
                    m
                    [:id]
                    [id]
                    (ts/default-timestamp-opt {}))]
        (is (= r record-foo))))

    (testing "propagates cassandra errors correctly"
      (is (thrown-with-msg?
           Exception
           #"select failed"
           @(m/select
             tu/*model-session*
             (assoc-in m [:primary-table :name] :select_test_wrong_table_name)
             [:id]
             [id]
             (ts/default-timestamp-opt {})))))))

(deftest select-one-test
  (let [m (create-simple-entity)
        id (uuid/v1)
        record-foo (ms/create-entity-instance {:id id :nick "foo"})
        _ (insert-record :simple_model_select_test record-foo)]

    (testing "selects a single record"
      (let [{r-id :id
             r-nick :nick
             :as r} @(m/select-one
                      tu/*model-session*
                      m
                      [:id]
                      [id]
                      (ts/default-timestamp-opt {}))]
        (is (= id r-id))
        (is (= "foo" r-nick))))

    (testing "returns nil if no match"
      (let [wrong-id (uuid/v1)
            r @(m/select-one
                tu/*model-session*
                m
                [:id]
                [wrong-id]
                (ts/default-timestamp-opt {}))]
        (is (nil? r))))

    (testing "propagates a cassandra error correctly"
      (is (thrown?
           Exception
           @(m/select-one
             tu/*model-session*
             ;; unknown table error will be thrown all the way
             ;; from the cassandra server
             (assoc-in m [:primary-table :name]

                       :ensure_one_test_wrong_table_name)
             [:id]
             [id]
             (ts/default-timestamp-opt {})))))))

(deftest prepare-select-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo (ms/create-entity-instance {:id id :nick "foo"})

        _ (insert-record :simple_model_select_test record-foo)]

    (testing "prepare-selecting record-foo"
      (let [[r e] @(m/select
                    tu/*model-session*
                    m
                    [:id]
                    [id]
                    (ts/default-timestamp-opt {:prepare? true}))]
        (is (= r record-foo))))))
