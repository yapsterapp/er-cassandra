(ns er-cassandra.model.alia.unique-key-test
  (:require
   [er-cassandra.model.util.test :as tu]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.unique-key :as uk]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(deftest applied?-test
  (is (= true (boolean (uk/applied? {(keyword "[applied]") true}))))
  (is (= false (boolean (uk/applied? {})))))

(deftest applied-or-owned?-test
  (let [e (t/create-entity
           {:primary-table {:name :foos :key [:id]}})
        oid (uuid/v1)]
    (is (= true (boolean (uk/applied-or-owned?
                          e [oid] {(keyword "[applied]") true}))))
    (is (= true (boolean (uk/applied-or-owned?
                          e [oid] {(keyword "[applied]") false :id oid}))))
    (is (= false (boolean (uk/applied-or-owned?
                           e [oid] {(keyword "[applied]") false :id (uuid/v1)}))))))



(deftest acquire-unique-key-test
  (let [_ (tu/create-table :unique_key_test
                           "(id timeuuid primary key, nick text)")
        _ (tu/create-table :unique_key_test_by_nick
                           "(nick text primary key, id timeuuid)")
        m (t/create-entity
           {:primary-table {:name :unique_key_test :key [:id]}
            :unique-key-tables [{:name :unique_key_test_by_nick :key [:nick]}]})

        [ida idb] [(uuid/v1) (uuid/v1)]

        _ (r/insert tu/*model-session* :unique_key_test {:id ida :nick "foo"})]

    (testing "acquire an uncontended unique-key"
      (let [[status report reason] @(uk/acquire-unique-key
                                     tu/*model-session*
                                     m
                                     (-> m :unique-key-tables first)
                                     [ida]
                                     ["foo"])]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]} report))
        (is (= :key/inserted reason))))

    (testing "acquiring an already owned unique-key"
      (let [[status key-desc reason] @(uk/acquire-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"])]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]} key-desc))
        (is (= :key/owned reason))))

    (testing "acquiring a stale ref"
      (let [;; remove the owning record so the ref is stale
            _ (r/delete tu/*model-session* :unique_key_test :id ida)
            _ (r/insert tu/*model-session* :unique_key_test {:id idb :nick "foo"})
            [status key-desc reason] @(uk/acquire-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [idb]
                                       ["foo"])]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [idb]
                :key [:nick]
                :key-value ["foo"]} key-desc))
        (is (= :key/updated reason))))

    (testing "failing to acquire"
      (let [[status key-desc reason] @(uk/acquire-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"])]
        (is (= :fail status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]} key-desc))
        (is (= :key/notunique reason))))))

(deftest release-unique-key-test
  (let [_ (tu/create-table :unique_key_test
                           "(id timeuuid primary key, nick text)")
        _ (tu/create-table :unique_key_test_by_nick
                           "(nick text primary key, id timeuuid)")
        m (t/create-entity
           {:primary-table {:name :unique_key_test :key [:id]}
            :unique-key-tables [{:name :unique_key_test_by_nick :key [:nick]}]})

        [ida idb] [(uuid/v1) (uuid/v1)]

        _ (r/insert tu/*model-session*
                    :unique_key_test
                    {:id ida :nick "foo"})
        _ (r/insert tu/*model-session*
                    :unique_key_test_by_nick
                    {:nick "foo" :id ida})]
    (testing "release an owned key"
      (let [[status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"])]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]}))
        (is (= :deleted reason))))

    (testing "releasing a non-existing key"
      (let [[status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"])]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]}))
        (is (= :stale reason))))

    (testing "attempting to release someone else's key"
      (let [;; first give the key back to ida
            _ (r/insert tu/*model-session*
                        :unique_key_test_by_nick
                        {:nick "foo" :id ida})
            [status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [idb]
                                       ["foo"])]

        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]}))
        (is (= :stale reason))))))

(deftest stale-unique-key-values-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :unique-key-tables [{:name :foos_by_bar :key [:bar]}
                                {:name :foos_by_baz
                                 :key [:baz]
                                 :collections {:baz :set}}]})]

    (testing "ignores unique keys when missing from new-record"
      (is (empty?
           (uk/stale-unique-key-values
            m
            {:id :a :bar :b}
            {:id :a}
            (-> m :unique-key-tables first)))))

    (testing "correctly identifies a stale singular unique key values"
      (is (= [[:b]]
             (uk/stale-unique-key-values
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :unique-key-tables first)))))

    (testing "correctly identifiers stale collection unique key values"
      (is (= #{[:b] [:d]}
             (set
              (uk/stale-unique-key-values
               m
               {:id :a :baz #{:b :c :d}}
               {:id :a :baz #{:c}}
               (-> m :unique-key-tables second))))))))

(defn create-singular-unique-key-entity
  []
  (tu/create-table :singular_unique_key_test
                     "(id timeuuid primary key, nick text)")
  (tu/create-table :singular_unique_key_test_by_nick
                     "(nick text primary key, id timeuuid)")

  (t/create-entity
   {:primary-table {:name :singular_unique_key_test :key [:id]}
    :unique-key-tables [{:name :singular_unique_key_test_by_nick
                         :key [:nick]}]}))

(defn create-set-unique-key-entity
  []
  (tu/create-table :set_unique_key_test
                   "(id timeuuid primary key, nick set<text>)")
  (tu/create-table :set_unique_key_test_by_nick
                   "(nick text primary key, id timeuuid)")
  (t/create-entity
   {:primary-table {:name :set_unique_key_test :key [:id]}
    :unique-key-tables [{:name :set_unique_key_test_by_nick
                         :key [:nick]
                         :collections {:nick :set}}]}))

(deftest release-stale-unique-keys-test
  (testing "singular unique key"
    (let [sm (create-singular-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]

          _ @(r/insert tu/*model-session*
                       :singular_unique_key_test
                       {:id ida :nick "foo"})
          _ @(r/insert tu/*model-session*
                       :singular_unique_key_test_by_nick
                       {:nick "foo" :id ida})]

      (testing "release singular stale unique key"
        (let [[[status key-desc reason]] @(uk/release-stale-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id ida :nick "foo"}
                                           {:id ida :nick nil})]
          (is (= :ok status))
          (is (= {:uber-key [:id] :uber-key-value [ida]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :deleted reason))))))

  (testing "release values from set unique key"
    (let [cm (create-set-unique-key-entity)

          [ida idb] [(uuid/v1) (uuid/v1)]
         _ @(r/insert tu/*model-session*
                       :set_unique_key_test
                       {:id ida :nick #{"foo" "bar" "baz"}})
          _ @(r/insert tu/*model-session*
                       :set_unique_key_test_by_nick
                       {:nick "foo" :id ida})
          _ @(r/insert tu/*model-session*
                       :set_unique_key_test_by_nick
                       {:nick "bar" :id ida})]

      (testing "release set stale unique key values"
        (let [r @(uk/release-stale-unique-keys
                  tu/*model-session*
                  cm
                  {:id ida :nick #{"foo" "bar" "baz"}}
                  {:id ida :nick #{"foo"}})]
          (is (= #{[:ok
                    {:uber-key [:id] :uber-key-value [ida]
                     :key [:nick] :key-value ["bar"]}
                    :deleted]
                   [:ok
                    {:uber-key [:id] :uber-key-value [ida]
                     :key [:nick] :key-value ["baz"]}
                    :stale]}
                 (set r))))))))

(deftest acquire-unique-keys-test
  (testing "acquire singular unique key"
    (let [sm (create-singular-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]

          _ @(r/insert tu/*model-session*
                       :singular_unique_key_test
                       {:id ida :nick "foo"})]

      (testing "acquire a singular unique key"
        (let [[[status key-desc reason]] @(uk/acquire-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id ida :nick "foo"})]
          (is (= :ok status))
          (is (= {:uber-key [:id] :uber-key-value [ida]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :key/inserted reason))))

      (testing "failing to acquire a singular unique key"
        (let [[[status key-desc reason]] @(uk/acquire-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id idb :nick "foo"})]
          (is (= :fail status))
          (is (= {:uber-key [:id] :uber-key-value [idb]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :key/notunique reason))))))

  (testing "acquire values in set unique key"
    (let [cm (create-set-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]

          _ @(r/insert tu/*model-session*
                       :set_unique_key_test
                       {:id ida :nick #{"foo" "bar"}})
          _ @(r/insert tu/*model-session*
                       :set_unique_key_test_by_nick
                       {:nick "foo" :id ida})]
      (testing "acquire values from a set of unique keys"
        (let [r @(uk/acquire-unique-keys
                  tu/*model-session*
                  cm
                  {:id ida :nick #{"foo" "bar"}})]
          (is (= #{[:ok
                    {:uber-key [:id] :uber-key-value [ida]
                     :key [:nick] :key-value ["foo"]}
                    :key/owned]
                   [:ok
                    {:uber-key [:id] :uber-key-value [ida]
                     :key [:nick] :key-value ["bar"]}
                    :key/inserted]}
                 (set r))))))))

(deftest update-with-acquire-responses-test
  )

(deftest describe-acquire-failures-test
  )

(deftest responses-for-key-test
  )

(deftest update-record-by-key-response-test
  )

(deftest without-unique-keys-test
  )

(deftest upsert-primary-record-without-unique-keys-test
  )

(deftest update-unique-keys-after-primary-upsert-test
  )

(deftest upsert-primary-record-and-update-unique-keys-test
  )
