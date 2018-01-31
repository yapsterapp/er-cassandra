(ns er-cassandra.model.alia.upsert-test
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
   [er-cassandra.model.alia.lookup :as l]
   [er-cassandra.model.alia.upsert :as u]
   [er-cassandra.model.alia.delete :as d]
   [prpr.promise :as pr]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_upsert_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_upsert_test :key [:id]}}))

(defn create-simple-entity-with-protected-column
  []
  (tu/create-table :simple_upsert_test_with_protected_column
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_upsert_test_with_protected_column :key [:id]}
    :callbacks
    {:before-save [(t/create-protect-columns-callback
                    :update-nick?
                    :nick)]}}))

(defn create-secondary-entity
  []
  (tu/create-table :secondary_upsert_test
                   "(org_id timeuuid, id timeuuid, nick text, primary key (org_id, id))")
  (tu/create-table :secondary_upsert_test_by_nick
                   "(org_id timeuuid, nick text, id timeuuid, primary key (org_id, nick))")
  (t/create-entity
   {:primary-table {:name :secondary_upsert_test :key [:org_id :id]}
    :secondary-tables [{:name :secondary_upsert_test_by_nick
                        :key [:org_id :nick]}]}))

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

(defn create-lookup-and-secondaries-entity
  []
  (tu/create-table
   :upsert_lookup_and_secondaries_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text, thing text, primary key (org_id, id))")
  (tu/create-table
   :upsert_lookup_and_secondaries_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, primary key (org_id, nick))")
  (tu/create-table
   :upsert_lookup_and_secondaries_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :upsert_lookup_and_secondaries_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :upsert_lookup_and_secondaries_test_by_thing
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text, thing text, primary key (org_id, thing))"
   )
  (t/create-entity
   {:primary-table {:name :upsert_lookup_and_secondaries_test :key [:org_id :id]}
    :lookup-tables [{:name :upsert_lookup_and_secondaries_test_by_nick
                         :key [:org_id :nick]}
                        {:name :upsert_lookup_and_secondaries_test_by_email
                         :key [:email]
                         :collections {:email :set}}
                        {:name :upsert_lookup_and_secondaries_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]
    :secondary-tables [{:name :upsert_lookup_and_secondaries_test_by_thing
                        :key [:org_id :thing]}]}))

(defn create-unique-lookup-secondaries-entity
  []
  (tu/create-table
   :upsert_unique_lookup_secondaries_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, thing text, title text, tag set<text>, dept list<text>, stuff text, primary key (org_id, id))")

  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, primary key (org_id, nick))")
  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")

  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_thing
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, thing text, title text, tag set<text>, dept list<text>, stuff text, primary key (org_id, thing))")

  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_title
   "(title text, org_id timeuuid, id timeuuid, primary key (org_id, title))")
  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_tag
   "(tag text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :upsert_unique_lookup_secondaries_test_by_dept
   "(dept text primary key, org_id timeuuid, id timeuuid)")

  (t/create-entity
   {:primary-table {:name :upsert_unique_lookup_secondaries_test :key [:org_id :id]}
    :unique-key-tables [{:name :upsert_unique_lookup_secondaries_test_by_nick
                         :key [:org_id :nick]}
                        {:name :upsert_unique_lookup_secondaries_test_by_email
                         :key [:email]
                         :collections {:email :set}}
                        {:name :upsert_unique_lookup_secondaries_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]
    :secondary-tables [{:name :upsert_unique_lookup_secondaries_test_by_thing
                        :key [:org_id :thing]}]
    :lookup-tables [{:name :upsert_unique_lookup_secondaries_test_by_title
                         :key [:org_id :title]}
                        {:name :upsert_unique_lookup_secondaries_test_by_tag
                         :key [:tag]
                         :collections {:tag :set}}
                        {:name :upsert_unique_lookup_secondaries_test_by_dept
                         :key [:dept]
                         :collections {:dept :list}}]}))

(deftest insert-index-record-test
  (let [m (create-simple-entity)
        id (uuid/v1)
        r {:id id :nick "foo"}
        [status
         record
         reason] @(u/insert-index-record tu/*model-session*
                                         m
                                         (:primary-table m)
                                         r
                                         (ts/default-timestamp-opt))]
    (is (= :ok status))
    (is (= r record))
    (is (= :upserted reason))
    (is (= r (fetch-record :simple_upsert_test :id id)))))


(defn create-secondary-entity-for-nil-non-key-cols
  []
  (tu/create-table :secondary_with_nil_non_key_cols_test
                   "(id uuid primary key, other_id uuid, nick text)")
  (tu/create-table :secondary_with_nil_non_key_cols_test_by_other_id
                   "(id timeuuid, other_id uuid, nick text, primary key (other_id, id))")
  (t/create-entity
   {:primary-table {:name :secondary_with_nil_non_key_cols_test :key [:id]}
    :secondary-tables [{:name :secondary_with_nil_non_key_cols_test_by_other_id
                        :key [:other_id :id]}]}))

;; unit test for a problem observed with relationship {:cascade :null}
;; it seems that if a record originally created with update is later
;; updated to have all nil non-pk cols then the record gets deleted
;; https://ajayaa.github.io/cassandra-difference-between-insert-update/
;; since this is undesirable for secondary tables, insert should be
;; used instead - this test ensures that insert is used
(deftest dont-delete-secondaries-with-nil-non-key-cols-test
  (let [m (create-secondary-entity-for-nil-non-key-cols)
        [id other-id] [(uuid/v1) (uuid/v1)]
        r {:id id :other_id other-id :nick "foo"}

        i1 (upsert-instance m r)
        f1 (fetch-record :secondary_with_nil_non_key_cols_test [:id] [id])
        fi1 (fetch-record :secondary_with_nil_non_key_cols_test_by_other_id
                          [:other_id :id] [other-id id])

        nnr (assoc r :nick nil)
        i2 (upsert-instance m nnr)

        f2 (fetch-record :secondary_with_nil_non_key_cols_test [:id] [id])
        fi2 (fetch-record :secondary_with_nil_non_key_cols_test_by_other_id
                          [:other_id :id] [other-id id])]
    (is (= r f1))
    (is (= r fi1))

    (is (= nnr f2))
    (is (= nnr fi2))))

(deftest upsert-secondaries-test
  (let [m (create-secondary-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]
        r {:org_id org-id :id id :nick "bar"}
        [status
         record
         reason] @(u/upsert-secondaries
                   tu/*model-session*
                   m
                   r
                   (ts/default-timestamp-opt))]
    (is (= r (fetch-record :secondary_upsert_test_by_nick [:org_id :nick] [org-id "bar"])))))


(deftest upsert-lookups-test
  (let [m (create-mixed-lookup-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]

        nick-foo-r {:org_id org-id :id id :nick "foo"}

        [status
         record
         reason] @(u/upsert-lookups
                   tu/*model-session*
                   m
                   nil
                   {:org_id org-id :id id
                    :nick "foo"
                    :email #{"foo@bar.com" "foo@baz.com"}
                    :phone ["123" "456"]}
                   (ts/default-timestamp-opt))]

    (is (= {:org_id org-id :id id :nick "foo"}
           (fetch-record :upsert_mixed_lookup_test_by_nick
                         [:org_id :nick] [org-id "foo"])))
    (is (= {:org_id org-id :id id :email "foo@bar.com"}
           (fetch-record :upsert_mixed_lookup_test_by_email
                         [:email] ["foo@bar.com"])))
    (is (= {:org_id org-id :id id :email "foo@baz.com"}
           (fetch-record :upsert_mixed_lookup_test_by_email
                         [:email] ["foo@baz.com"])))
    (is (= {:org_id org-id :id id :phone "123"}
           (fetch-record :upsert_mixed_lookup_test_by_phone
                         [:phone] "123")))
    (is (= {:org_id org-id :id id :phone "456"}
           (fetch-record :upsert_mixed_lookup_test_by_phone
                         [:phone] "456")))))

(deftest copy-unique-keys-test
  (let [m (t/create-entity
           {:primary-table {:name :upsert_copy_unique_keys_test :key [:org_id :id]}
            :unique-key-tables [{:name :upsert_copy_unique_keys_test_by_nick
                                 :key [:org_id :nick]}
                                {:name :upsert_copy_unique_keys_test_by_email
                                 :key [:email]
                                 :collections {:email :set}}
                                {:name :upsert_copy_unique_keys_test_by_phone
                                 :key [:phone]
                                 :collections {:phone :list}}]})
        [org-id id] [(clj-uuid/v1) (clj-uuid/v1)]
        r {:org_id org-id
           :id id
           :nick "foo"
           :email #{"foo@bar.com" "foo@baz.com"}
           :phone ["123" "456"]}]
    (is (= (select-keys r [:nick :email :phone])
           (u/copy-unique-keys
            m
            r
            {})))))

(deftest update-secondaries-and-lookups-test
  (let [m (create-lookup-and-secondaries-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]]

    (testing "first insert"
      (let [record {:org_id org-id
                    :id id
                    :nick "foo"
                    :stuff "whateva"
                    :thing "innit"
                    :email #{"foo@bar.com" "foo@baz.com"}
                    :phone ["123" "456"]}
            r @(u/update-secondaries-and-lookups
                tu/*model-session*
                m
                nil
                record
                (ts/default-timestamp-opt))]
        (is (= {:org_id org-id :id id :nick "foo"}
               (fetch-record :upsert_lookup_and_secondaries_test_by_nick
                             [:org_id :nick] [org-id "foo"])))
        (is (= record
               (fetch-record :upsert_lookup_and_secondaries_test_by_thing
                             [:org_id :thing] [org-id "innit"])))
        (is (= {:org_id org-id :id id :email "foo@bar.com"}
               (fetch-record :upsert_lookup_and_secondaries_test_by_email
                             [:email] ["foo@bar.com"])))
        (is (= {:org_id org-id :id id :email "foo@baz.com"}
               (fetch-record :upsert_lookup_and_secondaries_test_by_email
                             [:email] ["foo@baz.com"])))
        (is (= {:org_id org-id :id id :phone "123"}
               (fetch-record :upsert_lookup_and_secondaries_test_by_phone
                             [:phone] "123")))
        (is (= {:org_id org-id :id id :phone "456"}
               (fetch-record :upsert_lookup_and_secondaries_test_by_phone
                             [:phone] "456")))))))

(deftest upsert-changes*-test
  (let [m (create-unique-lookup-secondaries-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]

        record {:org_id org-id
                :id id

                :stuff "whateva"

                :nick "foo"
                :email #{"foo@bar.com" "foo@baz.com"}
                :phone ["123" "456"]

                :thing "blah"

                :title "mr"
                :tag #{"quick" "slow"}
                :dept ["hr" "dev"]}]

    (testing "upserts a record"
      (let [_ @(u/upsert-changes* tu/*model-session*
                                  m
                                  nil
                                  record
                                  (ts/default-timestamp-opt))]

        (is (= record (fetch-record :upsert_unique_lookup_secondaries_test
                                    [:org_id :id] [org-id id])))))

    (testing "result includes columns from old-record not in update"
      (let [new-record {:org_id org-id
                        :id id
                        :nick "bar"}
            [r acqf] @(u/upsert-changes* tu/*model-session*
                                         m
                                         record
                                         new-record
                                         (ts/default-timestamp-opt))]

        (is (= (merge record new-record)
               (fetch-record :upsert_unique_lookup_secondaries_test
                             [:org_id :id] [org-id id])))

        (is (= (merge record new-record)
               r))))

    (testing "result includes columns removed from an op by :before-save callbacks"
      (let [m (create-simple-entity-with-protected-column)

            [id] [(uuid/v1)]
            record {:id id :nick "foo"}
            [r acqf] @(u/upsert-changes* tu/*model-session*
                                         m
                                         nil
                                         record
                                         (ts/default-timestamp-opt))

            record-with-nil-nick {:id id :nick nil}]
        (is (= record-with-nil-nick r))
        (is (= record-with-nil-nick
               (fetch-record :simple_upsert_test_with_protected_column
                             [:id]
                             [id])))))

    (testing "keys which are not valid c* columns are removed from results"
      (let [m (create-simple-entity-with-protected-column)

            [id] [(uuid/v1)]
            upsert-record {:id id :nick "bar" :update-nick? true}
            [r acqf] @(u/upsert-changes* tu/*model-session*
                                         m
                                         nil
                                         upsert-record
                                         (ts/default-timestamp-opt))

            record-with-nick {:id id :nick "bar"}]
        (is (= record-with-nick r))
        (is (= record-with-nick
               (fetch-record :simple_upsert_test_with_protected_column
                             [:id]
                             [id])))))))

(deftest upsert-changes*-if-not-exists-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo {:id id :nick "foo"}
        record-bar {:id id :nick "bar"}]

    (testing "insert record-foo if-not-exists"
      (let [[r e] @(u/upsert-changes*
                    tu/*model-session*
                    m
                    nil
                    record-foo
                    (ts/default-timestamp-opt
                     {:if-not-exists true}))]
        (is (= record-foo r))))

    (testing "doesn't insert record-bar if-not-exists"
      (let [[r e] @(pr/catch-error
                    (u/upsert-changes*
                     tu/*model-session*
                     m
                     nil
                     record-bar
                     (ts/default-timestamp-opt
                      {:if-not-exists true})))

            fr (fetch-record :simple_upsert_test :id id)]
        (is (= :upsert/primary-record-upsert-error r))
        (is (= record-foo fr))))))

(deftest upsert-changes*-if-exists-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo {:id id :nick "foo"}
        record-bar {:id id :nick "bar"}

        _ (insert-record :simple_upsert_test record-bar)]

    (testing "upsert record-foo if-exists and it does exist"
      (let [[r e] @(u/upsert-changes*
                    tu/*model-session*
                    m
                    (dissoc record-foo :nick)
                    record-foo
                    (ts/default-timestamp-opt
                     {:if-exists true}))
            fr (fetch-record :simple_upsert_test :id id)]
        (is (= record-foo fr))))

    (testing "doesn't insert record-bar if-exists and it doesn't exist"
      (let [_ (delete-record :simple_upsert_test :id id)

            [r e] @(pr/catch-error
                    (u/upsert-changes*
                     tu/*model-session*
                     m
                     (dissoc record-bar :nick)
                     record-bar
                     (ts/default-timestamp-opt
                      {:if-exists true})))

            fr (fetch-record :simple_upsert_test :id id)]
        (is (= :upsert/primary-record-upsert-error r))
        (is (= nil fr))))))

(deftest upsert-changes*-only-if-test
  (let [m (create-simple-entity)
        id (uuid/v1)

        record-foo {:id id :nick "foo"}
        record-bar {:id id :nick "bar"}

        _ (insert-record :simple_upsert_test record-bar)]

    (testing "upsert record-foo if condition holds"
      (let [[r e] @(u/upsert-changes*
                    tu/*model-session*
                    m
                    (dissoc record-foo :nick)
                    record-foo
                    (ts/default-timestamp-opt
                     {:only-if [[:= :nick "bar"]]}))
            fr (fetch-record :simple_upsert_test :id id)]
        (is (= record-foo fr))))

    (testing "doesn't insert record-bar if condition fails"
      (let [[r e] @(pr/catch-error
                    (u/upsert-changes*
                     tu/*model-session*
                     m
                     (dissoc record-bar :nick)
                     record-bar
                     (ts/default-timestamp-opt
                      {:only-if [[:= :nick "bar"]]})))

            fr (fetch-record :simple_upsert_test :id id)]
        (is (= :upsert/primary-record-upsert-error r))
        (is (= record-foo fr))))))

(deftest upsert*-test
  (testing "upserts an instance of an entity with no foreign keys"
    (let [m (create-simple-entity)
          id (uuid/v1)

          record-foo {:id id :nick "foo"}

          [r e] @(u/upsert*
                  tu/*model-session*
                  m
                  record-foo
                  {})
          fr (fetch-record :simple_upsert_test :id id)]
      (is (= record-foo fr))))

  (testing "throws if given an entity with foreign keys"
    (let [m (create-secondary-entity)
          [org-id id] [(uuid/v1) (uuid/v1)]
          record-bar {:org_id org-id :id id :nick "bar"}

          [r e] @(pr/catch-error
                  (u/upsert*
                   tu/*model-session*
                   m
                   record-bar
                   {}))
          fr (fetch-record :secondary_upsert_test [:org_id :id] [org-id id])]
      (is (= r :upsert/require-explicit-select-upsert))
      (is (= fr nil))))
  )

(deftest select-upsert*-test
  (let [m (create-mixed-lookup-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]]

    (testing "upserts an instance of an entity with foreign keys"
      (let [record {:org_id org-id :id id
                    :nick "foo"
                    :email #{"foo@bar.com" "foo@baz.com"}
                    :phone ["123" "456"]
                    :stuff nil}
            [r e] @(u/select-upsert*
                    tu/*model-session*
                    m
                    record
                    {})]
        (is (= record
               (fetch-record :upsert_mixed_lookup_test [:org_id :id] [org-id id])))

        (is (= {:org_id org-id :id id :nick "foo"}
               (fetch-record :upsert_mixed_lookup_test_by_nick
                             [:org_id :nick] [org-id "foo"])))
        (is (= {:org_id org-id :id id :email "foo@bar.com"}
               (fetch-record :upsert_mixed_lookup_test_by_email
                             [:email] ["foo@bar.com"])))
        (is (= {:org_id org-id :id id :email "foo@baz.com"}
               (fetch-record :upsert_mixed_lookup_test_by_email
                             [:email] ["foo@baz.com"])))
        (is (= {:org_id org-id :id id :phone "123"}
               (fetch-record :upsert_mixed_lookup_test_by_phone
                             [:phone] "123")))
        (is (= {:org_id org-id :id id :phone "456"}
               (fetch-record :upsert_mixed_lookup_test_by_phone
                             [:phone] "456")))))

    (testing "correctly updates foreign keys"
      (let [record-b {:org_id org-id :id id
                      :nick "bar"
                      :email #{"foo@bar.com" "bar@baz.com"}
                      :phone ["456" "789"]
                      :stuff "blah"}
            [r e] @(u/select-upsert*
                    tu/*model-session*
                    m
                    record-b
                    {})
            fr (fetch-record :upsert_mixed_lookup_test [:org_id :id] [org-id id])]
        (is (= record-b
               (fetch-record :upsert_mixed_lookup_test [:org_id :id] [org-id id])))

        (is (= nil
               (fetch-record :upsert_mixed_lookup_test_by_nick
                             [:org_id :nick] [org-id "foo"])))
        (is (= {:org_id org-id :id id :nick "bar"}
               (fetch-record :upsert_mixed_lookup_test_by_nick
                             [:org_id :nick] [org-id "bar"])))


        (is (= {:org_id org-id :id id :email "foo@bar.com"}
               (fetch-record :upsert_mixed_lookup_test_by_email
                             [:email] ["foo@bar.com"])))
        (is (= nil
               (fetch-record :upsert_mixed_lookup_test_by_email
                             [:email] ["foo@baz.com"])))
        (is (= {:org_id org-id :id id :email "bar@baz.com"}
               (fetch-record :upsert_mixed_lookup_test_by_email
                             [:email] ["bar@baz.com"])))


        (is (= nil
               (fetch-record :upsert_mixed_lookup_test_by_phone
                             [:phone] "123")))
        (is (= {:org_id org-id :id id :phone "456"}
               (fetch-record :upsert_mixed_lookup_test_by_phone
                             [:phone] "456")))
        (is (= {:org_id org-id :id id :phone "789"}
               (fetch-record :upsert_mixed_lookup_test_by_phone
                             [:phone] "789")))))))

(deftest change*-test
  (testing "does nothing if record hasn't changed"
    (let [m (create-simple-entity)
          id (uuid/v1)

          record-foo {:id id :nick "foo"}

          [k r] @(u/change*
                  tu/*model-session*
                  m
                  record-foo
                  record-foo
                  {})
          fr (fetch-record :simple_upsert_test :id id)]
      (is (= :noop k))
      (is (= record-foo r))
      (is (= nil fr))))
  (testing "deletes if record is nil"
    (let [m (create-simple-entity)
          id (uuid/v1)

          record-foo {:id id :nick "foo"}
          _ (upsert-instance m record-foo)
          fr1 (fetch-record :simple_upsert_test :id id)

          [k r] @(u/change*
                  tu/*model-session*
                  m
                  record-foo
                  nil
                  {})
          fr2 (fetch-record :simple_upsert_test :id id)]
      (is (= k :delete))
      (is (= record-foo r))
      (is (= record-foo fr1))
      (is (= nil fr2))))

  (testing "upsert-changes if record is non-nil and there are changes"
    (let [m (create-simple-entity)
          id (uuid/v1)

          record-foo {:id id :nick "foo"}
          record-bar (assoc record-foo :nick "bar")
          _ (upsert-instance m record-foo)
          fr1 (fetch-record :simple_upsert_test :id id)

          [k r acqf] @(u/change*
                       tu/*model-session*
                       m
                       record-foo
                       record-bar
                       {})
          fr2 (fetch-record :simple_upsert_test :id id)]
      (is (= :upsert k))
      (is (= record-bar r))
      (is (= record-foo fr1))
      (is (= (assoc record-foo :nick "bar") fr2))))

  (testing "acquire failures are returned"
    (let [m (create-unique-lookup-secondaries-entity)
          [org-id foo-id bar-id] [(uuid/v1) (uuid/v1) (uuid/v1)]

          record-foo {:org_id org-id :id foo-id
                      :nick "foo"
                      :email #{"foo@bar.com" "foo@baz.com"}
                      :phone ["123" "456"]
                      :thing nil
                      :title nil
                      :tag #{}
                      :dept []
                      :stuff nil}

          record-bar {:org_id org-id :id bar-id
                      :nick "foo"
                      :email #{"bar@bar.com" "foo@baz.com"}
                      :phone ["123" "789"]
                      :thing nil
                      :title nil
                      :tag #{}
                      :dept []
                      :stuff nil}

          unconflicted-bar {:org_id org-id :id bar-id
                            :nick nil
                            :email #{"bar@bar.com"}
                            :phone ["789"]
                            :thing nil
                            :title nil
                            :tag #{}
                            :dept []
                            :stuff nil}

          _ (upsert-instance m record-foo)

          [k r acqf] @(u/change*
                       tu/*model-session*
                       m
                       nil
                       record-bar
                       {})
          fr (fetch-record :upsert_unique_lookup_secondaries_test
                           [:org_id :id]
                           [org-id bar-id])]
      (is (= unconflicted-bar fr))
      (is (= :upsert k))
      (is (= unconflicted-bar r))
      (is (= 3 (count acqf)))
      (is (= #{[[:org_id :nick] [org-id "foo"]]
               [[:email] ["foo@baz.com"]]
               [[:phone] ["123"]]
               }
             (->> acqf (map second) (map (juxt :key :key-value)) set))))))
