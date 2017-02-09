(ns er-cassandra.model.alia.unique-key-test
  (:require
   [er-cassandra.model.util.test :as tu :refer [fetch-record]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.record :as r]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.unique-key :as uk]
   [qbits.hayt :as h]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

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

(defn create-mixed-unique-key-entity
  []
  (tu/create-table
   :mixed_unique_key_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text,  primary key (org_id, id))")
  (tu/create-table
   :mixed_unique_key_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, primary key (org_id, nick))")
  (tu/create-table
   :mixed_unique_key_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :mixed_unique_key_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")
  (t/create-entity
   {:primary-table {:name :mixed_unique_key_test :key [:org_id :id]}
    :unique-key-tables [{:name :mixed_unique_key_test_by_nick
                         :key [:org_id :nick]}
                        {:name :mixed_unique_key_test_by_email
                         :key [:email]
                         :collections {:email :set}}
                        {:name :mixed_unique_key_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]}))


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

        _ (tu/insert-record :unique_key_test {:id ida :nick "foo"})]

    (testing "acquire an uncontended unique-key"
      (let [[status report reason] @(uk/acquire-unique-key
                                     tu/*model-session*
                                     m
                                     (-> m :unique-key-tables first)
                                     [ida]
                                     ["foo"]
                                     (ts/default-timestamp-opt))]
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
                                       ["foo"]
                                       (ts/default-timestamp-opt))]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]} key-desc))
        (is (= :key/owned reason))))

    (testing "acquiring a stale ref"
      (let [;; remove the owning record so the ref is stale
            _ (tu/delete-record :unique_key_test :id ida)
            _ (tu/insert-record :unique_key_test {:id idb :nick "foo"})
            [status key-desc reason] @(uk/acquire-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [idb]
                                       ["foo"]
                                       (ts/default-timestamp-opt))]
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
                                       ["foo"]
                                       (ts/default-timestamp-opt))]
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

        _ (tu/insert-record :unique_key_test {:id ida :nick "foo"})
        _ (tu/insert-record :unique_key_test_by_nick {:nick "foo" :id ida})]
    (testing "release an owned key"
      (let [[status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"]
                                       (ts/default-timestamp-opt))
            _ (Thread/sleep 1000)
            dr (fetch-record :unique_key_test_by_nick :nick "foo")]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]}))
        (is (= :deleted reason))
        (is (= nil dr))))

    (testing "releasing a non-existing key"
      (let [[status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [ida]
                                       ["foo"]
                                       (ts/default-timestamp-opt))]
        (is (= :ok status))
        (is (= {:uber-key (t/uber-key m)
                :uber-key-value [ida]
                :key [:nick]
                :key-value ["foo"]}))
        (is (= :stale reason))))

    (testing "attempting to release someone else's key"
      (let [;; first give the key back to ida
            _ (tu/insert-record :unique_key_test_by_nick {:nick "foo" :id ida})
            [status key-desc reason] @(uk/release-unique-key
                                       tu/*model-session*
                                       m
                                       (-> m :unique-key-tables first)
                                       [idb]
                                       ["foo"]
                                       (ts/default-timestamp-opt))]

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



(deftest release-stale-unique-keys-test
  (testing "singular unique key"
    (let [sm (create-singular-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]

          _ (tu/insert-record :singular_unique_key_test {:id ida :nick "foo"})
          _ (tu/insert-record :singular_unique_key_test_by_nick
                              {:nick "foo" :id ida})]

      (testing "release singular stale unique key"
        (let [[[status key-desc reason]] @(uk/release-stale-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id ida :nick "foo"}
                                           {:id ida :nick nil}
                                           (ts/default-timestamp-opt))]
          (is (= :ok status))
          (is (= {:uber-key [:id] :uber-key-value [ida]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :deleted reason))))))

  (testing "release values from set unique key"
    (let [cm (create-set-unique-key-entity)

          [ida idb] [(uuid/v1) (uuid/v1)]
          _ (tu/insert-record :set_unique_key_test
                              {:id ida :nick #{"foo" "bar" "baz"}})
          _ (tu/insert-record :set_unique_key_test_by_nick
                              {:nick "foo" :id ida})
          _ (tu/insert-record :set_unique_key_test_by_nick
                              {:nick "bar" :id ida})]

      (testing "release set stale unique key values"
        (let [r @(uk/release-stale-unique-keys
                  tu/*model-session*
                  cm
                  {:id ida :nick #{"foo" "bar" "baz"}}
                  {:id ida :nick #{"foo"}}
                  (ts/default-timestamp-opt))]
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

          _ (tu/insert-record :singular_unique_key_test
                              {:id ida :nick "foo"})]

      (testing "acquire a singular unique key"
        (let [[[status key-desc reason]] @(uk/acquire-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id ida :nick "foo"}
                                           (ts/default-timestamp-opt))]
          (is (= :ok status))
          (is (= {:uber-key [:id] :uber-key-value [ida]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :key/inserted reason))))

      (testing "failing to acquire a singular unique key"
        (let [[[status key-desc reason]] @(uk/acquire-unique-keys
                                           tu/*model-session*
                                           sm
                                           {:id idb :nick "foo"}
                                           (ts/default-timestamp-opt))]
          (is (= :fail status))
          (is (= {:uber-key [:id] :uber-key-value [idb]
                  :key [:nick] :key-value ["foo"]} key-desc))
          (is (= :key/notunique reason))))))

  (testing "acquire values in set unique key"
    (let [cm (create-set-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]

          _ (tu/insert-record :set_unique_key_test
                              {:id ida :nick #{"foo" "bar"}})
          _ (tu/insert-record :set_unique_key_test_by_nick
                              {:nick "foo" :id ida})]
      (testing "acquire values from a set of unique keys"
        (let [r @(uk/acquire-unique-keys
                  tu/*model-session*
                  cm
                  {:id ida :nick #{"foo" "bar"}}
                  (ts/default-timestamp-opt))]
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
  (testing "removing singular unacquired value"
    (let [sm (create-singular-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]
          r (uk/update-with-acquire-responses
             (-> sm :unique-key-tables first)
             [[:fail
               {:uber-key [:id] :uber-key-value [ida]
                :key [:nick] :key-value ["foo"]}
               :key/notunique]]
             {:id ida
              :nick "foo"})]
      (is (= {:id ida :nick nil} r))))

  (testing "removing unacquired collection values"
    (let [cm (create-set-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]
          r (uk/update-with-acquire-responses
             (-> cm :unique-key-tables first)
             [[:fail
               {:uber-key [:id] :uber-key-value [ida]
                :key [:nick] :key-value ["foo"]}
               :key/notunique]]
             {:id ida
              :nick #{"foo" "bar"}})]
      (is (= {:id ida :nick #{"bar"}} r)))))

(deftest describe-acquire-failures-test
  (testing "describe singular acquisition failure"
    (let [m (create-singular-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]]
      (is (= [[:key/notunique
               {:tag :key/notunique,
                :message ":nick is not unique: foo",
                :type :key,
                :primary-table :singular_unique_key_test,
                :uber-key-value [ida],
                :key [:nick],
                :key-value ["foo"]}]]
             (uk/describe-acquire-failures
              m
              {:id ida :nick "foo"}
              [[:fail
                {:uber-key [:id] :uber-key-value [ida]
                 :key [:nick] :key-value ["foo"]}
                :key/notunique]])))))
  (testing "describe set value acquisition failure"
    (let [m (create-set-unique-key-entity)
          [ida idb] [(uuid/v1) (uuid/v1)]]
      (is (= [[:key/notunique
               {:tag :key/notunique,
                :message ":nick is not unique: foo",
                :type :key,
                :primary-table :set_unique_key_test,
                :uber-key-value [ida],
                :key [:nick],
                :key-value ["foo"]}]
              [:key/notunique
               {:tag :key/notunique,
                :message ":nick is not unique: bar",
                :type :key,
                :primary-table :set_unique_key_test,
                :uber-key-value [ida],
                :key [:nick],
                :key-value ["bar"]}]]
             (uk/describe-acquire-failures
              m
              {:id ida :nick #{"foo" "bar" "baz"}}
              [[:fail
                {:uber-key [:id] :uber-key-value [ida]
                 :key [:nick] :key-value ["foo"]}
                :key/notunique]
               [:fail
                {:uber-key [:id] :uber-key-value [ida]
                 :key [:nick] :key-value ["bar"]}
                :key/notunique]]))))))

(deftest responses-for-key-test
  (let [ida (uuid/v1)]
    (is (= [[:fail
             {:uber-key [:id] :uber-key-value [ida]
              :key [:nick] :key-value ["foo"]}
             :key/notunique]]
           (uk/responses-for-key
            [:nick]
            [[:fail
              {:uber-key [:id] :uber-key-value [ida]
               :key [:nick] :key-value ["foo"]}
              :key/notunique]
             [:fail
              {:uber-key [:id] :uber-key-value [ida]
               :key [:blah] :key-value ["bar"]}
              :key/notunique]])))))

(deftest update-record-by-key-response-test
  (testing "updating record according to acquire responses"
    (let [m (t/create-entity
             {:primary-table {:name :unique_key_test :key [:id]}
              :unique-key-tables [{:name :unique_key_test_by_nick
                                   :key [:nick]}
                                  {:name :unique_key_test_by_email
                                   :key [:email]
                                   :collections {:email :set}}]})
          ida (uuid/v1)]
      (is (= {:id ida
              :nick nil
              :email #{"foo@bar.com"}}
             (uk/update-record-by-key-responses
              m
              nil
              {:id ida
               :nick "foo"
               :email #{"foo@bar.com" "foo@baz.com"}}
              [[:fail
                {:uber-key [:id] :uber-key-value [ida]
                 :key [:nick] :key-value ["foo"]}
                :key/notunique]
               [:fail
                {:uber-key [:id] :uber-key-value [ida]
                 :key [:email] :key-value ["foo@baz.com"]}
                :key/notunique]]))))))

(deftest without-unique-keys-test
  (let [m (t/create-entity
           {:primary-table {:name :unique_key_test :key [:org_id :id]}
            :unique-key-tables [{:name :unique_key_test_by_nick
                                 :key [:org_id :nick]}
                                {:name :unique_key_test_by_email
                                 :key [:org_id :email]
                                 :collections {:email :set}}]})
        [org-id ida] [(uuid/v1) (uuid/v1)]]
    (is (= {:org_id org-id
            :id ida}
           (uk/without-unique-keys
            m
            {:org_id org-id
             :id ida
             :nick "foo"
             :email #{"foo@bar.com" "foo@baz.com"}})))))

(deftest upsert-primary-record-without-unique-keys-test
  (let [_ (tu/create-table :upsert_primary_without_unique_keys_test
                           "(id timeuuid primary key, nick text, a text, b text)")

        m (t/create-entity
           {:primary-table {:name :upsert_primary_without_unique_keys_test :key [:id]}
            :unique-key-tables [{:name :upsert_primary_without_unique_keys_test_by_nick
                                 :key [:nick]}]})]
    (testing "simple insert"
      (let [id (uuid/v1)
            r @(uk/upsert-primary-record-without-unique-keys
                tu/*model-session*
                m
                {:id id :nick "foo" :a "ana" :b "anb"}
                (ts/default-timestamp-opt))]
        (is (= [{:id id :nick nil :a "ana" :b "anb"} nil] r))))

    (testing "simple insert with a timestamp & ttl"
      (let [id (uuid/v1)
            r @(uk/upsert-primary-record-without-unique-keys
                tu/*model-session*
                m
                {:id id :nick "foo" :a "ana" :b "anb"}
                {:using {:ttl 10
                         :timestamp 1000}})
            fr @(r/select-one
                 tu/*model-session*
                 :upsert_primary_without_unique_keys_test
                 [:id]
                 [id]
                 {:columns [:id :nick :a :b
                            (h/as (h/cql-fn :ttl :a) :a_ttl)
                            (h/as (h/cql-fn :writetime :a) :a_writetime)]})]
        (is (= [{:id id :nick nil :a "ana" :b "anb"} nil] r))
        (is (= {:id id :nick nil :a "ana" :b "anb"}
               (select-keys fr [:id :nick :a :b])))
        (is (some? (:a_ttl fr)))
        (is (= (> (:a_ttl fr) 5)))
        (is (some? (:a_writetime fr)))
        (is (= (> (:a_writetime fr) 5)))))

    (testing "update existing record"
      (let [id (uuid/v1)
            _ (tu/insert-record :upsert_primary_without_unique_keys_test
                                {:id id :nick "blah" :a "olda" :b "oldb"})
            r @(uk/upsert-primary-record-without-unique-keys
                tu/*model-session*
                m
                {:id id :nick "foo" :a "newa"}
                (ts/default-timestamp-opt))]
        (is (= [{:id id :nick "blah" :a "newa" :b "oldb"} nil] r))))

    (testing "with if-not-exists"

      (let [[id id-b] [(uuid/v1) (uuid/v1)]]
        (testing "if it doesn't already exist"
          (let [r @(uk/upsert-primary-record-without-unique-keys
                    tu/*model-session*
                    m
                    {:id id :nick "foo" :a "ana" :b "anb"}
                    (ts/default-timestamp-opt
                     {:if-not-exists true}))]
            (is (= [{:id id :a "ana" :b "anb"} nil] r))))

        (testing "with a TTL"
          (let [r @(uk/upsert-primary-record-without-unique-keys
                    tu/*model-session*
                    m
                    {:id id-b :nick "foo" :a "ana" :b "anb"}
                    (ts/default-timestamp-opt
                     {:if-not-exists true
                      :using {:ttl 10}}))
                fr @(r/select-one
                     tu/*model-session*
                     :upsert_primary_without_unique_keys_test
                     [:id]
                     [id-b]
                     {:columns [:id :nick :a :b (h/as (h/cql-fn :ttl :a) :a_ttl)]})]
            (is (= [{:id id-b :a "ana" :b "anb"} nil] r))
            (is (= {:id id-b :nick nil :a "ana" :b "anb"}
                   (select-keys fr [:id :nick :a :b])))
            (is (some? (:a_ttl fr)))
            (is (= (> (:a_ttl fr) 5)))))

        (testing "if it does already exist"
          (let [r @(uk/upsert-primary-record-without-unique-keys
                    tu/*model-session*
                    m
                    {:id id :nick "foo" :a "ana" :b "anb"}
                    (ts/default-timestamp-opt
                     {:if-not-exists true}))]
            (is (= [nil [[:upsert/primary-record-upsert-error
                          {:record
                           {:id id,
                            :nick "foo",
                            :a "ana",
                            :b "anb"},
                           :if-not-exists true,
                           :only-if nil,
                           :tag :upsert/primary-record-upsert-error,
                           :message "couldn't upsert primary record"}]]]
                   r))))))

    (testing "with only-if"
      (testing "if it already exists"
        (let [id (uuid/v1)
              _ (tu/insert-record :upsert_primary_without_unique_keys_test
                                  {:id id :nick "bloogh" :a "olda" :b "oldb"})
              r @(uk/upsert-primary-record-without-unique-keys
                  tu/*model-session*
                  m
                  {:id id :nick "bloogh" :a "newa" :b "newb"}
                  (ts/default-timestamp-opt
                   {:only-if [[:= :nick "bloogh"]]}))]
          (is (= [{:id id :nick "bloogh" :a "newa" :b "newb"} nil]
                 r))))

      (testing "if it doesn't already exist"
        (let [id (uuid/v1)
              r @(uk/upsert-primary-record-without-unique-keys
                  tu/*model-session*
                  m
                  {:id id :nick "foogle" :a "newa" :b "newb"}
                  (ts/default-timestamp-opt
                   {:only-if [[:= :nick "blah"]]}))]
          (is (= [nil
                  [[:upsert/primary-record-upsert-error
                    {:record
                     {:id id,
                      :nick "foogle",
                      :a "newa",
                      :b "newb"},
                     :if-not-exists nil,
                     :only-if [[:= :nick "blah"]],
                     :tag :upsert/primary-record-upsert-error,
                     :message "couldn't upsert primary record"}]]]
                 r)))
        ))))

(deftest update-unique-keys-after-primary-upsert-test
  (testing "with mixed unique keys"
    (let [m (create-mixed-unique-key-entity)
          [org-id ida idb] [(uuid/v1) (uuid/v1) (uuid/v1)]]
      (testing "acquire some keys"
        (let [_ (tu/insert-record :mixed_unique_key_test
                                  {:org_id org-id
                                   :id ida})
              updated-a {:org_id org-id
                         :id ida
                         :stuff "blah"
                         :nick "foo"
                         :email #{"foo@bar.com"}
                         :phone ["123456"]}
              [record
               acquire-failures] @(uk/update-unique-keys-after-primary-upsert
                                   tu/*model-session*
                                   m
                                   {:org_id org-id :id ida}
                                   updated-a
                                   (ts/default-timestamp-opt))]
          (is (= updated-a record))
          (is (empty? acquire-failures))

          (is (= updated-a
                 (fetch-record :mixed_unique_key_test [:org_id :id] [org-id ida])))
          (is (= {:org_id org-id :id ida :nick "foo"}
                 (fetch-record :mixed_unique_key_test_by_nick
                               [:org_id :nick] [org-id "foo"])))
          (is (= {:email "foo@bar.com" :org_id org-id :id ida}
                 (fetch-record :mixed_unique_key_test_by_email
                               :email "foo@bar.com")))
          (is (= {:phone "123456" :org_id org-id :id ida}
                 (fetch-record :mixed_unique_key_test_by_phone
                               :phone "123456")))))

      (testing "mixed acquire / failure"
        (let [_ (tu/insert-record :mixed_unique_key_test
                                  {:org_id org-id
                                   :id idb})

              [record
               acquire-failures] @(uk/update-unique-keys-after-primary-upsert
                                   tu/*model-session*
                                   m
                                   {:org_id org-id :id idb}
                                   {:org_id org-id
                                    :id idb
                                    :stuff "boo"
                                    :nick "foo"
                                    :email #{"foo@bar.com" "bar@baz.com" "blah@bloo.com"}
                                    :phone ["123456" "09876" "777777"]}
                                   (ts/default-timestamp-opt))
              updated-b {:org_id org-id
                         :id idb
                         :stuff "boo"
                         :nick nil
                         :email #{"bar@baz.com" "blah@bloo.com"}
                         :phone ["09876" "777777"]}]
          (is (= updated-b record))
          (is (= #{[:key/notunique
                    {:tag :key/notunique,
                     :message ":phone is not unique: 123456",
                     :type :key,
                     :primary-table :mixed_unique_key_test,
                     :uber-key-value [org-id idb],
                     :key [:phone],
                     :key-value ["123456"]}]
                   [:key/notunique
                    {:tag :key/notunique,
                     :message ":nick is not unique: foo",
                     :type :key,
                     :primary-table :mixed_unique_key_test,
                     :uber-key-value [org-id idb],
                     :key [:org_id :nick],
                     :key-value [org-id "foo"]}]
                   [:key/notunique
                    {:tag :key/notunique,
                     :message ":email is not unique: foo@bar.com",
                     :type :key,
                     :primary-table :mixed_unique_key_test,
                     :uber-key-value [org-id idb],
                     :key [:email],
                     :key-value ["foo@bar.com"]}]}
                 (set acquire-failures)))

          (is (= updated-b
                 (fetch-record :mixed_unique_key_test
                               [:org_id :id] [org-id idb])))
          ;; ida keys remain with ida
          (is (= {:nick "foo" :org_id org-id :id ida}
                 (fetch-record :mixed_unique_key_test_by_nick
                               [:org_id :nick] [org-id "foo"])))
          (is (= {:phone "123456" :org_id org-id :id ida}
                 (fetch-record :mixed_unique_key_test_by_phone
                               :phone "123456")))
          (is (= {:email "foo@bar.com" :org_id org-id :id ida}
                 (fetch-record :mixed_unique_key_test_by_email :email "foo@bar.com")))

          ;; idb gets the new keys
          (is (= {:phone "09876" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_phone :phone "09876")))
          (is (= {:phone "777777" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_phone :phone "777777")))
          (is (= {:email "bar@baz.com" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_email :email "bar@baz.com")))
          (is (= {:email "blah@bloo.com" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_email :email "blah@bloo.com")))))

      (testing "updates and removals"
        (let [updated-b {:org_id org-id
                         :id idb
                         :stuff "fooo"
                         :nick "bar"
                         :email #{"bar@baz.com" "woo@woo.com"}
                         :phone ["777777" "111111"]}

              old-r (fetch-record :mixed_unique_key_test
                                  [:org_id :id] [org-id idb])

              [record
               acquire-failures] @(uk/update-unique-keys-after-primary-upsert
                                   tu/*model-session*
                                   m
                                   old-r
                                   updated-b
                                   (ts/default-timestamp-opt))]

          (is (= updated-b
                 record))
          (is (empty? acquire-failures))

          (is (= updated-b
                 (fetch-record :mixed_unique_key_test [:org_id :id] [org-id idb])))

          ;; stale keys
          (is (= nil
                 (fetch-record :mixed_unique_key_test_by_phone :phone "09876")))
          (is (= nil
                 (fetch-record :mixed_unique_key_test_by_email :email "blah@bloo.com")))

          ;; preserved and new keys
          (is (= {:phone "777777" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_phone :phone "777777")))
          (is (= {:phone "111111" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_phone :phone "111111")))
          (is (= {:email "bar@baz.com" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_email :email "bar@baz.com")))
          (is (= {:email "woo@woo.com" :org_id org-id :id idb}
                 (fetch-record :mixed_unique_key_test_by_email :email "woo@woo.com"))))))))


(deftest upsert-primary-record-and-update-unique-keys-test
  (let [m (create-mixed-unique-key-entity)
        [org-id ida idb] [(uuid/v1) (uuid/v1) (uuid/v1)]]
    (testing "success"
      (let [updated-a {:org_id org-id
                       :id ida
                       :stuff "stuff"
                       :nick "foo"
                       :email #{"foo@bar.com"}
                       :phone ["123456"]}
            [record
             acquire-failures] @(uk/upsert-primary-record-and-update-unique-keys
                                 tu/*model-session*
                                 m
                                 updated-a
                                 (ts/default-timestamp-opt))]
        (is (= updated-a record))
        (is (empty? acquire-failures))
        (is (= updated-a (fetch-record :mixed_unique_key_test
                                       [:org_id :id] [org-id ida])))))
    (testing "if-not-exists failure"
      (let [old-a (fetch-record :mixed_unique_key_test
                                [:org_id :id] [org-id ida])
            updated-a {:org_id org-id
                       :id ida
                       :stuff "blah"
                       :nick "foofoo"
                       :email #{"foofoo@bar.com"}
                       :phone ["12345654321"]}
            [record
             errors] @(uk/upsert-primary-record-and-update-unique-keys
                                 tu/*model-session*
                                 m
                                 updated-a
                                 (ts/default-timestamp-opt
                                  {:if-not-exists true}))]
        (is (= nil record))
        (is (= [[:upsert/primary-record-upsert-error
                 {:record updated-a,
                  :if-not-exists true,
                  :only-if nil,
                  :tag :upsert/primary-record-upsert-error,
                  :message "couldn't upsert primary record"}]]
               errors))
        (is (= old-a (fetch-record :mixed_unique_key_test
                                   [:org_id :id] [org-id ida])))))

    (testing "only-if failure"
      (let [old-a (fetch-record :mixed_unique_key_test
                                [:org_id :id] [org-id ida])
            updated-a {:org_id org-id
                       :id ida
                       :stuff "wha"
                       :nick "foofoo"
                       :email #{"foofoo@bar.com"}
                       :phone ["12345654321"]}
            [record
             errors] @(uk/upsert-primary-record-and-update-unique-keys
                                 tu/*model-session*
                                 m
                                 updated-a
                                 (ts/default-timestamp-opt
                                  {:only-if [[:= :nick "bar"]]}))]
        (is (= nil record))
        (is (= [[:upsert/primary-record-upsert-error
                 {:record updated-a,
                  :if-not-exists nil,
                  :only-if [[:= :nick "bar"]],
                  :tag :upsert/primary-record-upsert-error,
                  :message "couldn't upsert primary record"}]]
               errors))
        (is (= old-a (fetch-record :mixed_unique_key_test
                                   [:org_id :id] [org-id ida])))))))
