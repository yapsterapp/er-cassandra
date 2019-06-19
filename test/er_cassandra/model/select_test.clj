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
        (is (= r record-foo))))))

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
