(ns er-cassandra.model.alia.upsert-test
  (:require
   [er-cassandra.model.util.test :as tu :refer [fetch-record]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.upsert :as u]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_upsert_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_upsert_test :key [:id]}}))

(deftest delete-record-test
  (let [m (create-simple-entity)
        id (uuid/v1)
        _ @(r/insert tu/*model-session* :simple_upsert_test {:id id :nick "foo"})
        [status
         detail
         reason] @(u/delete-record tu/*model-session* m (:primary-table m) [id])]
    (is (= :ok status))
    (is (= {:table :simple_upsert_test
            :key [:id]
            :key-value [id]} detail))
    (is (= :deleted reason))
    (is (= nil (fetch-record :simple_upsert_test :id id)))))

(deftest upsert-record-test
  (let [m (create-simple-entity)
        id (uuid/v1)
        r {:id id :nick "foo"}
        [status
         record
         reason] @(u/upsert-record tu/*model-session* m (:primary-table m) r)]
    (is (= :ok status))
    (is (= r record))
    (is (= :upserted reason))
    (is (= r (fetch-record :simple_upsert_test :id id)))))

(deftest stale-lookup-key-values-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-key-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           (u/stale-lookup-key-values
            m
            {:id :a :bar :b}
            {:id :a}
            (-> m :lookup-key-tables first)))))

    (testing "correctly identifies a stale singular lookup key values"
      (is (= [[:b]]
             (u/stale-lookup-key-values
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :lookup-key-tables first)))))

    (testing "correctly identifiers stale collection lookup key values"
      (is (= #{[:b] [:d]}
             (set
              (u/stale-lookup-key-values
               m
               {:id :a :baz #{:b :c :d}}
               {:id :a :baz #{:c}}
               (-> m :lookup-key-tables second))))))))

(deftest stale-secondary-key-value-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}]})
        st (-> m :secondary-tables first)]

    (testing "ignores secondary keys when missing from new-record"
      (is (= nil
             (u/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a}
              (-> m :secondary-tables first)))))

    (testing "correctly identifies a stale singular secondary key value"
      (is (= [:b]
             (u/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :secondary-tables first)))))))

(deftest with-columns-option-for-lookups
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_baz :key [:baz]}]
            :lookup-key-tables [{:name :foos_by_x
                                 :key [:x]
                                 :with-columns [:c1 :c2]}]})]

    (testing "lookup-keys with a full record supplied"
      (let [record {:id 1 :bar "bar1" :baz "baz1" :c1 :C1 :c2 :C2 :c3 :C3 :x "x-key"}
            [[t lrecord] & _ :as lookups] (u/lookup-record-seq m nil record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          false  (contains? lrecord :c3))))

    (testing "lookup-keys with a partial record supplied"
      (let [old-record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 :c2 :C2 :c3 :C3 }
            record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 }
            [[t lrecord] & _ :as lookups] (u/lookup-record-seq m old-record record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          false  (contains? lrecord :c3))))))
