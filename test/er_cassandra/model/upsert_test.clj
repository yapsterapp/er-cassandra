(ns er-cassandra.model.upsert-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [manifold.stream :as stream]
   [er-cassandra.session.alia :as als]
   [er-cassandra.record :as r]
   [er-cassandra.model :as m]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [prpr.promise :as pr]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_model_upsert_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_model_upsert_test :key [:id]}}))

(deftest upsert-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo {:id id :nick "foo"}
        record-bar {:id id :nick "bar"}

        _ (insert-record :simple_model_upsert_test record-bar)]

    (testing "upsert record-foo if condition holds"
      (let [[r e] @(m/upsert
                    tu/*model-session*
                    m
                    record-foo
                    (ts/default-timestamp-opt
                     {:only-if [[:= :nick "bar"]]}))
            fr (fetch-record :simple_model_upsert_test :id id)]
        (is (= record-foo fr))))

    (testing "doesn't insert record-bar if condition fails"
      (let [[r e] @(pr/catch-error
                    (m/upsert
                     tu/*model-session*
                     m
                     record-bar
                     (ts/default-timestamp-opt
                      {:only-if [[:= :nick "bar"]]})))

            fr (fetch-record :simple_model_upsert_test :id id)]
        (is (= :upsert/primary-record-upsert-error r))
        (is (= record-foo fr))))))

(deftest prepare-upsert-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo {:id id :nick "foo"}]

    (testing "upsert record-foo with prepared statements"
      (let [[r e] @(m/upsert
                    tu/*model-session*
                    m
                    record-foo
                    (ts/default-timestamp-opt
                     {:prepare? true}))
            fr (fetch-record :simple_model_upsert_test :id id)]
        (is (= record-foo fr))))))

(deftest upsert-buffered-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]

        record-foo {:id id-foo :nick "foo"}
        record-bar {:id id-bar :nick "bar"}]

    (testing "upsert record-foo if condition holds"
      (let [r-s @(m/upsert-buffered
                  tu/*model-session*
                  m
                  [record-foo record-bar])
            r @(->> r-s
                    (stream/map first)
                    (stream/reduce conj []))
            f-foo (fetch-record :simple_model_upsert_test :id id-foo)
            f-bar (fetch-record :simple_model_upsert_test :id id-bar)]
        (is (= record-foo f-foo))
        (is (= record-bar f-bar))
        (is (= (set r)
               (set [record-foo record-bar])))))))

(deftest upsert-many-test
  (let [m (create-simple-entity)
        [id-foo id-bar] [(uuid/v1) (uuid/v1)]

        record-foo {:id id-foo :nick "foo"}
        record-bar {:id id-bar :nick "bar"}]

    (testing "upsert record-foo if condition holds"
      (let [rr @(m/upsert-many
                 tu/*model-session*
                 m
                 [record-foo record-bar])
            r (->> rr
                   (map first))
            f-foo (fetch-record :simple_model_upsert_test :id id-foo)
            f-bar (fetch-record :simple_model_upsert_test :id id-bar)]
        (is (= record-foo f-foo))
        (is (= record-bar f-bar))
        (is (= (set r)
               (set [record-foo record-bar])))))))
