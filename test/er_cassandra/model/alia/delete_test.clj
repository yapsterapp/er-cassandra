(ns er-cassandra.model.alia.delete-test
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
   [er-cassandra.model.alia.upsert :as u]
   [er-cassandra.model.alia.lookup :as l]
   [er-cassandra.model.alia.delete :as d]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-entity
  []
  (tu/create-table :simple_delete_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :simple_delete_test :key [:id]}}))

(defn create-secondary-entity
  []
  (tu/create-table :secondary_delete_test
                   "(org_id timeuuid, id timeuuid, nick text, primary key (org_id, id))")
  (tu/create-table :secondary_delete_test_by_nick
                   "(org_id timeuuid, nick text, id timeuuid, primary key (org_id, nick))")
  (t/create-entity
   {:primary-table {:name :secondary_delete_test :key [:org_id :id]}
    :secondary-tables [{:name :secondary_delete_test_by_nick
                        :key [:org_id :nick]}]}))

(defn create-mixed-lookup-entity
  []
  (tu/create-table
   :delete_mixed_lookup_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text,  thing text, primary key (org_id, id))")
  (tu/create-table
   :delete_mixed_lookup_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, email set<text>, phone list<text>, stuff text, thing text, primary key (org_id, nick))")
  (tu/create-table
   :delete_mixed_lookup_test_by_thing
   "(thing text, org_id timeuuid, id timeuuid, primary key (org_id, thing))")
  (tu/create-table
   :delete_mixed_lookup_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :delete_mixed_lookup_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")
  (t/create-entity
   {:primary-table {:name :delete_mixed_lookup_test :key [:org_id :id]}
    :unique-key-tables []
    :secondary-tables [{:name :delete_mixed_lookup_test_by_nick
                        :key [:org_id :nick]}]
    :lookup-tables [{:name :delete_mixed_lookup_test_by_thing
                     :key [:org_id :thing]}
                    {:name :delete_mixed_lookup_test_by_phone
                     :key [:phone]
                     :collections {:phone :list}}
                    {:name :delete_mixed_lookup_test_by_email
                     :key [:email]
                     :collections {:email :set}}]}))

(deftest delete-index-record-test
  (let [m (create-simple-entity)
        id (uuid/v1)
        _ (tu/insert-record :simple_delete_test {:id id :nick "foo"})
        [status
         detail
         reason] @(d/delete-index-record tu/*model-session*
                                         m
                                         (:primary-table m)
                                         [id]
                                         (ts/default-timestamp-opt))]
    (is (= :ok status))
    (is (= {:table :simple_delete_test
            :key [:id]
            :key-value [id]} detail))
    (is (= :deleted reason))
    (is (= nil (fetch-record :simple_delete_test :id id)))))

(deftest stale-secondary-key-value-test
  (let [m (t/create-entity
           {:primary-table {:name :foos :key [:id]}
            :secondary-tables [{:name :foos_by_bar :key [:bar]}]})
        st (-> m :secondary-tables first)]

    (testing "ignores secondary keys when missing from new-record"
      (is (= nil
             (d/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a}
              (-> m :secondary-tables first)))))

    (testing "correctly identifies a stale singular secondary key value"
      (is (= [:b]
             (d/stale-secondary-key-value
              m
              {:id :a :bar :b}
              {:id :a :bar nil}
              (-> m :secondary-tables first)))))))

(deftest stale-secondary-key-value-with-null-non-key-cols-test
  (let [m (t/create-entity
           {:primary-table {:name :secondary_with_nil_non_key_cols_test :key [:id]}
            :secondary-tables [{:name :secondary_with_nil_non_key_cols_test_by_other_id
                                :key [:other_id :id]}]})
        [id other-id] [:an-id :an-other-id]]
    (is (= nil
           (d/stale-secondary-key-value
            m
            {:id id :other_id other-id :nick "foo"}
            {:id id :other_id other-id :nick nil}
            (-> m :secondary-tables first))))))

(deftest delete-stale-secondaries-test
  (let [m (create-secondary-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]
        r {:org_id org-id :id id :nick "foo"}
        _ (tu/insert-record :secondary_delete_test_by_nick r)
        old-r (fetch-record :secondary_delete_test_by_nick [:org_id :nick] [org-id "foo"])
        [status
         record
         reason] @(d/delete-stale-secondaries
                   tu/*model-session*
                   m
                   old-r
                   {:org_id org-id :id id :nick "bar"}
                   (ts/default-timestamp-opt))]
    (is (= r old-r))
    (is (= nil (fetch-record :secondary_delete_test_by_nick [:org_id :nick] [org-id "foo"])))))

(deftest delete-stale-lookups-test
  (let [m (create-mixed-lookup-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]

        thing-foo-r {:org_id org-id :id id :thing "Tfoo"}

        _ (insert-record :delete_mixed_lookup_test_by_thing thing-foo-r)
        _ (is (= thing-foo-r (fetch-record
                              :delete_mixed_lookup_test_by_thing
                              [:org_id :thing] [org-id "Tfoo"])))

        email-foobar-r {:org_id org-id :id id :email "foo@bar.com"}
        _ (insert-record :delete_mixed_lookup_test_by_email email-foobar-r)
        _ (is (= email-foobar-r (fetch-record
                                 :delete_mixed_lookup_test_by_email
                                 [:email] ["foo@bar.com"])))
        email-foobaz-r {:org_id org-id :id id :email "foo@baz.com"}
        _ (insert-record :delete_mixed_lookup_test_by_email email-foobaz-r)
        _ (is (= email-foobaz-r (fetch-record
                                 :delete_mixed_lookup_test_by_email
                                 [:email] ["foo@baz.com"])))

        phone-123-r {:org_id org-id :id id :phone "123"}
        _ (insert-record :delete_mixed_lookup_test_by_phone phone-123-r)
        _ (is (= phone-123-r (fetch-record
                              :delete_mixed_lookup_test_by_phone
                              [:phone] "123")))

        phone-456-r {:org_id org-id :id id :phone "456"}
        _ (insert-record :delete_mixed_lookup_test_by_phone phone-456-r)
        _ (is (= phone-456-r (fetch-record
                              :delete_mixed_lookup_test_by_phone
                              [:phone] "456")))

        [status
         record
         reason] @(d/delete-stale-lookups
                   tu/*model-session*
                   m
                   {:org_id org-id :id id
                    :thing "Tfoo"
                    :nick "foo"
                    :email #{"foo@bar.com" "foo@baz.com"}
                    :phone ["123" "456"]}
                   {:org_id org-id :id id
                    :thing "Tbar"
                    :nick "bar"
                    :email #{"foo@bar.com" "blah@wah.com"}
                    :phone ["123" "789"]}
                   (ts/default-timestamp-opt))]

    (is (= nil (fetch-record :delete_mixed_lookup_test_by_thing
                             [:org_id :thing] [org-id "Tfoo"])))
    (is (= email-foobar-r (fetch-record :delete_mixed_lookup_test_by_email
                                        [:email] ["foo@bar.com"])))
    (is (= nil (fetch-record :delete_mixed_lookup_test_by_email
                             [:email] ["foo@baz.com"])))
    (is (= phone-123-r (fetch-record :delete_mixed_lookup_test_by_phone
                                     [:phone] "123")))
    (is (= nil (fetch-record :delete_mixed_lookup_test_by_phone
                             [:phone] "456")))))


(deftest delete-mixed-lookup-entity-test
  (let [m (create-mixed-lookup-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]

        r {:org_id org-id
           :id id
           :thing "Thang"
           :nick "foo"
           :email #{"foo@bar.com" "foo@baz.com"}
           :phone ["01234" "5678"]
           :stuff "blah"}

        ;; check all expected records are created
        _ @(u/select-upsert* tu/*model-session* m r {})
        f-r (tu/fetch-record :delete_mixed_lookup_test [:org_id :id] [org-id id])
        f-nick (tu/fetch-record :delete_mixed_lookup_test_by_nick
                                [:org_id :nick] [org-id "foo"])
        f-thing (tu/fetch-record :delete_mixed_lookup_test_by_thing
                                 [:org_id :thing] [org-id "Thang"])
        f-email-bar (tu/fetch-record :delete_mixed_lookup_test_by_email
                                     :email "foo@bar.com")
        f-email-baz (tu/fetch-record :delete_mixed_lookup_test_by_email
                                     :email "foo@baz.com")
        f-phone-0 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                   :phone "01234")
        f-phone-5 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                   :phone "5678")

        d-r @(d/delete* tu/*model-session* m [:org_id :id] [org-id id] {})

        ;; check all expected records are deleted
        f2-r (tu/fetch-record :delete_mixed_lookup_test [:org_id :id] [org-id id])
        f2-nick (tu/fetch-record :delete_mixed_lookup_test_by_nick
                                 [:org_id :nick] [org-id "foo"])
        f2-thing (tu/fetch-record :delete_mixed_lookup_test_by_thing
                                  [:org_id :thing] [org-id "Thang"])
        f2-email-bar (tu/fetch-record :delete_mixed_lookup_test_by_email
                                      :email "foo@bar.com")
        f2-email-baz (tu/fetch-record :delete_mixed_lookup_test_by_email
                                      :email "foo@baz.com")
        f2-phone-0 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                    :phone "01234")
        f2-phone-5 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                    :phone "5678")
        ]

    (is (= f-r r))
    (is (= f-nick r))
    (is (= f-thing {:org_id org-id :id id :thing "Thang"}))
    (is (= f-email-bar {:org_id org-id :email "foo@bar.com" :id id}))
    (is (= f-email-baz {:org_id org-id :email "foo@baz.com" :id id}))
    (is (= f-phone-0 {:org_id org-id :phone "01234" :id id}))
    (is (= f-phone-5 {:org_id org-id :phone "5678" :id id}))

    (is (= f2-r nil))
    (is (= f2-nick nil))
    (is (= f2-thing nil))
    (is (= f2-email-bar nil))
    (is (= f2-email-baz nil))
    (is (= f2-phone-0 nil))
    (is (= f2-phone-5 nil))))
