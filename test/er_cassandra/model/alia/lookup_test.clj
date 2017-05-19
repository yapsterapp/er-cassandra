(ns er-cassandra.model.alia.lookup-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.session.alia :as als]
   [er-cassandra.record :as r]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.lookup :as l]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-mixed-lookup-entity
  []
  (tu/create-table
   :upsert_mixed_lookup_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text,  primary key (org_id, id))")
  (tu/create-table
   :upsert_mixed_lookup_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, primary key (org_id, nick))")
  (tu/create-table
   :upsert_mixed_lookup_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :upsert_mixed_lookup_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")
  (t/create-entity
   {:primary-table {:name :upsert_mixed_lookup_test :key [:org_id :id]}
    :lookup-tables [{:name :upsert_mixed_lookup_test_by_nick
                         :key [:org_id :nick]}
                        {:name :upsert_mixed_lookup_test_by_email
                         :key [:email]
                         :collections {:email :set}}
                        {:name :upsert_mixed_lookup_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]}))

(deftest stale-lookup-key-values-for-table-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           @(l/stale-lookup-key-values-for-table
             tu/*model-session*
             m
             {:id :a :bar :b}
             {:id :a}
             (-> m :lookup-tables first)))))

    (testing "treats nil new-record as deletion for singular key values"
      (is (= [[:b]]
             @(l/stale-lookup-key-values-for-table
               tu/*model-session*
               m
               {:id :a :bar :b}
               nil
               (-> m :lookup-tables first)))))

    (testing "correctly identifies a stale singular lookup key values"
      (is (= [[:b]]
             @(l/stale-lookup-key-values-for-table
               tu/*model-session*
               m
               {:id :a :bar :b}
               {:id :a :bar nil}
               (-> m :lookup-tables first)))))

    (testing "correctly identifiers stale collection lookup key values"
      (is (= #{[:b] [:d]}
             (set
              @(l/stale-lookup-key-values-for-table
                tu/*model-session*
                m
                {:id :a :baz #{:b :c :d}}
                {:id :a :baz #{:c}}
                (-> m :lookup-tables second))))))

    (testing "correctly identifiers stale collection lookup key values on delete"
      (is (= #{[:b] [:c] [:d]}
             (set
              @(l/stale-lookup-key-values-for-table
                tu/*model-session*
                m
                {:id :a :baz #{:b :c :d}}
                nil
                (-> m :lookup-tables second))))))))
