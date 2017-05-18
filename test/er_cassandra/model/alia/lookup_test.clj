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

(deftest stale-lookup-key-values-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :lookup-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores lookup keys when missing from new-record"
      (is (empty?
           (l/stale-lookup-key-values
            m
            {:id :a :bar :b}
            {:id :a}
            (-> m :lookup-tables first)))))

    (testing "treats nil new-record as deletion for singular key values"
      (is (= [[:b]]
             (l/stale-lookup-key-values
              m
              {:id :a :bar :b}
              nil
              (-> m :lookup-tables first)))))

    (testing "correctly identifies a stale singular lookup key values"
      (is (= [[:b]]
             (l/stale-lookup-key-values
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :lookup-tables first)))))

    (testing "correctly identifiers stale collection lookup key values"
      (is (= #{[:b] [:d]}
             (set
              (l/stale-lookup-key-values
               m
               {:id :a :baz #{:b :c :d}}
               {:id :a :baz #{:c}}
               (-> m :lookup-tables second))))))

    (testing "correctly identifiers stale collection lookup key values on delete"
      (is (= #{[:b] [:c] [:d]}
             (set
              (l/stale-lookup-key-values
               m
               {:id :a :baz #{:b :c :d}}
               nil
               (-> m :lookup-tables second))))))))

(deftest lookup-record-seq-test
  (testing "mixed lookups seq"
    (let [m (create-mixed-lookup-entity)
          nick-t (->> m :lookup-tables
                      (filter #(= :upsert_mixed_lookup_test_by_nick (:name %)))
                      first)
          email-t (->> m :lookup-tables
                       (filter #(= :upsert_mixed_lookup_test_by_email (:name %)))
                       first)
          phone-t (->> m :lookup-tables
                       (filter #(= :upsert_mixed_lookup_test_by_phone (:name %)))
                       first)
          [org-id id] [(uuid/v1) (uuid/v1)]
          r {:org_id org-id :id id
             :nick "foo"
             :email #{"foo@bar.com" "foo@baz.com"}
             :phone ["123" "456"]}

          rs (l/lookup-record-seq m nil r)]

      (is (= (set
              [[nick-t {:org_id org-id :id id :nick "foo"}]
               [email-t {:org_id org-id :id id :email "foo@bar.com"}]
               [email-t {:org_id org-id :id id :email "foo@baz.com"}]
               [phone-t {:org_id org-id :id id :phone "123"}]
               [phone-t {:org_id org-id :id id :phone "456"}]])
             (set rs)))))

  (testing "with-columns [cols] option"
    (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_baz :key [:baz]}]
            :lookup-tables [{:name :foos_by_x
                                 :key [:x]
                                 :with-columns [:c1 :c2]}]})]

    (testing "lookup-keys with-columns [:c1 c2] with a full record supplied"
      (let [record {:id 1 :bar "bar1" :baz "baz1" :c1 :C1 :c2 :C2 :c3 :C3 :x "x-key"}
            [[t lrecord] & _ :as lookups] (l/lookup-record-seq m nil record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          false  (contains? lrecord :c3))))

    (testing "lookup-keys with-columns [:c1 c2] with a partial record supplied"
      (let [old-record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 :c2 :C2 :c3 :C3 }
            record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 }
            [[t lrecord] & _ :as lookups] (l/lookup-record-seq m old-record record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          false  (contains? lrecord :c3))))))

  (testing "with-columns :all option"
    (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}
                               {:name :foos_by_baz :key [:baz]}]
            :lookup-tables [{:name :foos_by_x
                                 :key [:x]
                                 :with-columns :all}]})]

    (testing "lookup-keys with-columns :all with a full record supplied"
      (let [record {:id 1 :bar "bar1" :baz "baz1" :c1 :C1 :c2 :C2 :c3 :C3 :x "x-key"}
            [[t lrecord] & _ :as lookups] (l/lookup-record-seq m nil record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          :C3    (:c3 lrecord))))

    (testing "lookup-keys with-columns :all with a partial record supplied"
      (let [old-record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 :c2 :C2 :c3 :C3 }
            record {:id 1 :bar "bar1" :baz "baz1" :x "x-key" :c1 :C1 }
            [[t lrecord] & _ :as lookups] (l/lookup-record-seq m old-record record)]

        (are [x y] (= x y)
          1      (count lookups)
          :C1    (:c1 lrecord)
          :C2    (:c2 lrecord)
          :C3    (:c3 old-record)))))))
