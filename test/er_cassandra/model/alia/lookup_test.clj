(ns er-cassandra.model.alia.lookup-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [clojure.string :as str]
   [manifold.deferred :as d]
   [er-cassandra.session.alia :as als]
   [er-cassandra.record :as r]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.lookup :as l]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

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

(deftest insert-and-update-lookup-records-for-table-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-tables [{:name :foos_by_bar :key [:bar]}
                            {:name :foos_by_baz
                             :key [:baz]
                             :collections {:baz :set}}
                            {:name :foos_by_blah
                             :key [:blah]
                             :with-columns [:bar]}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           @(l/insert-and-update-lookup-records-for-table
             tu/*model-session*
             m
             {:id :a :bar :b}
             {:id :a}
             (-> m :lookup-tables first)))))

    (testing "correctly identifies a new singular lookup key value"
      (is (= [{:id :a :bar :b}]
             @(l/insert-and-update-lookup-records-for-table
               tu/*model-session*
               m
               {:id :a}
               {:id :a :bar :b}
               (-> m :lookup-tables first)))))

    (testing "correctly identifies new collection lookup key values"
      (is (= #{{:id :a :baz :b}
               {:id :a :baz :d}}
             (set
              @(l/insert-and-update-lookup-records-for-table
                tu/*model-session*
                m
                {:id :a :baz #{:c :x}}
                {:id :a :baz #{:b :c :d}}
                (-> m :lookup-tables second))))))

    (testing "correctly ignores lookups with unchanged extra cols"
      (is (= #{}
             (set
              @(l/insert-and-update-lookup-records-for-table
                tu/*model-session*
                m
                {:id :a :blah :dog :bar :cat}
                {:id :a :blah :dog :bar :cat}
                (-> m :lookup-tables (nth 2)))))))

    (testing "correctly identifies lookups with changed extra cols"
      (is (= #{{:id :a :blah :dog :bar :crocodile}}
             (set
              @(l/insert-and-update-lookup-records-for-table
                tu/*model-session*
                m
                {:id :a :blah :dog :bar :cat}
                {:id :a :blah :dog :bar :crocodile}
                (-> m :lookup-tables (nth 2)))))))))

(deftest generate-secondary-changes-for-table-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}]})]
    (testing "ignores lookup keys when missing from new record"
      (is (empty?
           @(l/generate-secondary-changes-for-table
             tu/*model-session*
             m
             (-> m :secondary-tables first)
             {:id :a :bar :b}
             {:id :a}))))

    (testing "nil key in old-record and new-record is empty"
      (is (= nil
             @(l/generate-secondary-changes-for-table
               tu/*model-session*
               m
               (-> m :secondary-tables first)
               {:id :a :bar nil}
               {:id :a :bar nil}))))

    (testing "nil key in old-record but not in new-record is delete"
      (is (= #{[{:id :a :bar :b} nil]}
             (set
              @(l/generate-secondary-changes-for-table
                tu/*model-session*
                m
                (-> m :secondary-tables first)
                {:id :a :bar :b}
                {:id :a :bar nil})))))

    (testing "nil key in new-record but not in old-record is create"
      (is (= #{[nil {:id :a :bar :c}]}
             (set
              @(l/generate-secondary-changes-for-table
                tu/*model-session*
                m
                (-> m :secondary-tables first)
                {:id :a :bar nil}
                {:id :a :bar :c})))))

    (testing "extra cols in create"
      (is (= #{[nil {:id :a :bar :c :foo 10}]}
             (set
              @(l/generate-secondary-changes-for-table
                tu/*model-session*
                m
                (-> m :secondary-tables first)
                {:id :a :bar nil}
                {:id :a :bar :c :foo 10})))))

    (testing "different key in old-record and new-record is delete and create"
      (is (= #{[{:id :a :bar :b} nil]
               [nil {:id :a :bar :c :foo 10}]}
             (set
              @(l/generate-secondary-changes-for-table
                tu/*model-session*
                m
                (-> m :secondary-tables first)
                {:id :a :bar :b}
                {:id :a :bar :c :foo 10})))))

    (testing "same key in new-record and old-record is update"
      (is (= #{[{:id :a :bar :b :foo 10} {:id :a :bar :b :foo 20}]}
             (set
              @(l/generate-secondary-changes-for-table
                tu/*model-session*
                m
                (-> m :secondary-tables first)
                {:id :a :bar :b :foo 10}
                {:id :a :bar :b :foo 20})))))))

(deftest generate-lookup-changes-for-table-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-tables [{:name :foos_by_bar :key [:bar]}
                            {:name :foos_by_baz
                             :key [:baz]
                             :collections {:baz :set}}
                            {:name :foos_by_blah
                             :key [:blah]
                             :with-columns [:bar]}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           @(l/generate-lookup-changes-for-table
             tu/*model-session*
             m
             (-> m :lookup-tables first)
             {:id :a :bar :b}
             {:id :a}))))

    (testing "treats nil new-record as deletion for singular key values"
      (is (= #{[{:id :a :bar :b} nil]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables first)
                {:id :a :bar :b}
                nil)))))

    (testing "correctly identifies a stale singular lookup key values"
      (is (= #{[{:id :a :bar :b} nil]}
             (set @(l/generate-lookup-changes-for-table
                   tu/*model-session*
                   m
                   (-> m :lookup-tables first)
                   {:id :a :bar :b}
                   {:id :a :bar nil})))))

    (testing "correctly identifiers stale collection lookup key values"
      (is (= #{[{:id :a :baz :b} nil]
               [{:id :a :baz :c} {:id :a :baz :c}]
               [{:id :a :baz :d} nil]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables second)
                {:id :a :baz #{:b :c :d}}
                {:id :a :baz #{:c}})))))

    (testing "correctly identifiers stale collection lookup key values on delete"
      (is (= #{[{:id :a :baz :b} nil]
               [{:id :a :baz :c} nil]
               [{:id :a :baz :d} nil]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables second)
                {:id :a :baz #{:b :c :d}}
                nil)))))

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           @(l/generate-lookup-changes-for-table
             tu/*model-session*
             m
             (-> m :lookup-tables first)
             {:id :a :bar :b}
             {:id :a}))))

    (testing "correctly identifies a new singular lookup key value"
      (is (= #{[nil {:id :a :bar :b}]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables first)
                {:id :a}
                {:id :a :bar :b})))))

    (testing "correctly identifies new collection lookup key values"
      (is (= #{[{:id :a :baz :c} {:id :a :baz :c}]
               [{:id :a :baz :x} nil]
               [nil {:id :a :baz :b}]
               [nil {:id :a :baz :d}]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables second)
                {:id :a :baz #{:c :x}}
                {:id :a :baz #{:b :c :d}})))))

    (testing "correctly identifies lookups with unchanged extra cols"
      (is (= #{[{:id :a :blah :dog :bar :cat} {:id :a :blah :dog :bar :cat}]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables (nth 2))
                {:id :a :blah :dog :bar :cat}
                {:id :a :blah :dog :bar :cat})))))

    (testing "correctly identifies lookups with changed extra cols"
      (is (= #{[{:id :a :blah :dog :bar :cat} {:id :a :blah :dog :bar :crocodile}]}
             (set
              @(l/generate-lookup-changes-for-table
                tu/*model-session*
                m
                (-> m :lookup-tables (nth 2))
                {:id :a :blah :dog :bar :cat}
                {:id :a :blah :dog :bar :crocodile})))))))

(defn generator-fn-lookup-test-generator
  [session entity table old-record
   {id :id
    r-name :name
    :as record}]
  (for [s (str/split r-name #"\s+")]
    {:id id :name s}))

(defn create-generator-fn-lookup-entity
  []
  (tu/create-table
   :generator_fn_lookup_test
   "(id uuid primary key, name text)")
  (tu/create-table
   :generator_fn_lookup_test_by_name
   "(name_char text primary key, id uuid)")
  (t/create-entity
   {:primary-table {:name :generator_fn_lookup_test :key [:id]}
    :lookup-tables [{:name :generator_fn_lookup_test_by_name_char
                     :key [:name]
                     :generator-fn generator-fn-lookup-test-generator}]}))


(deftest lookup-record-generator-fn-test
  (let [m (create-generator-fn-lookup-entity)
        id (uuid/v1)

        lookups (l/generate-lookup-records-for-table
                 tu/*model-session*
                 m
                 (-> m :lookup-tables first)
                 nil
                 {:id id
                  :name "foo bar baz"})]

    (testing "creates the expected lookups"
      (is (= #{{:id id :name "foo"}
               {:id id :name "bar"}
               {:id id :name "baz"}}
             (set lookups))))))
