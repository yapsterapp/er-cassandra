(ns er-cassandra.model.delete-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [prpr.stream :as stream]
   [er-cassandra.model :as m]
   [er-cassandra.model.types :as t]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_model_delete_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_model_delete_test :key [:id]}}))

(deftest delete-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]
        record-foo {:id id-foo :nick "foo"}
        _ (insert-record :simple_model_delete_test record-foo)]

    (testing "delete record"
      (let [[r-code
             r-record
             r-action] @(m/delete
                         tu/*model-session*
                         m
                         :id
                         id-foo)
            fr (fetch-record :simple_model_delete_test :id id-foo)]
        (is (= nil fr))
        (is (= r-code :ok))
        (is (= (into {} r-record) record-foo)) ;; record / map equivalence
        (is (= r-action :deleted))))))

(deftest prepare-delete-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]
        record-foo {:id id-foo :nick "foo"}
        _ (insert-record :simple_model_delete_test record-foo)]

    (testing "delete record"
      (let [[r-code
             r-record
             r-action] @(m/delete
                         tu/*model-session*
                         m
                         :id
                         id-foo
                         {:prepare? true})
            fr (fetch-record :simple_model_delete_test :id id-foo)]
        (is (= nil fr))
        (is (= r-code :ok))
        (is (= (into {} r-record) record-foo)) ;; record / map equivalence
        (is (= r-action :deleted))))))

(deftest delete-buffered-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]

        record-foo {:id id-foo :nick "foo"}
        record-bar {:id id-bar :nick "bar"}

        _ (insert-record :simple_model_delete_test record-foo)
        _ (insert-record :simple_model_delete_test record-bar)]

    (testing "delete-buffered"
      (let [r-s @(m/delete-buffered
                  tu/*model-session*
                  m
                  :id
                  [record-foo record-bar])
            r @(->> r-s
                    (stream/reduce conj []))
            f-foo (fetch-record :simple_model_delete_test :id id-foo)
            f-bar (fetch-record :simple_model_delete_test :id id-bar)]
        (is (= nil f-foo))
        (is (= nil f-bar))
        (is (= (set (for [[c r a] r] [c (into {} r) a]))
               (set [[:ok record-bar :deleted]
                     [:ok record-foo :deleted]])))))))

(deftest delete-many-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]

        record-foo {:id id-foo :nick "foo"}
        record-bar {:id id-bar :nick "bar"}

        _ (insert-record :simple_model_delete_test record-foo)
        _ (insert-record :simple_model_delete_test record-bar)]

    (testing "delete-many"
      (let [r @(m/delete-many
                  tu/*model-session*
                  m
                  :id
                  [record-foo record-bar])

            f-foo (fetch-record :simple_model_delete_test :id id-foo)
            f-bar (fetch-record :simple_model_delete_test :id id-bar)]
        (is (= nil f-foo))
        (is (= nil f-bar))
        (is (= (set (for [[c r a] r] [c (into {} r) a]))
               (set [[:ok record-bar :deleted]
                     [:ok record-foo :deleted]])))))))
