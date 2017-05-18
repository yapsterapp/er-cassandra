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

(defn create-mixed-lookup-entity
  []
  (tu/create-table
   :delete_mixed_lookup_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, stuff text,  primary key (org_id, id))")
  (tu/create-table
   :delete_mixed_lookup_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, email set<text>, phone list<text>, stuff text, primary key (org_id, nick))")
  (tu/create-table
   :delete_mixed_lookup_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :delete_mixed_lookup_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")
  (t/create-entity
   {:primary-table {:name :delete_mixed_lookup_test :key [:org_id :id]}
    :unique-key-tables [{:name :delete_mixed_lookup_test_by_email
                         :key [:email]
                         :collections {:email :set}}]
    :secondary-tables [{:name :delete_mixed_lookup_test_by_nick
                        :key [:org_id :nick]}]
    :lookup-key-tables [{:name :delete_mixed_lookup_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]}))

(deftest delete-mixed-lookup-entity-test
  (let [m (create-mixed-lookup-entity)
        [org-id id] [(uuid/v1) (uuid/v1)]

        r {:org_id org-id
           :id id
           :nick "foo"
           :email #{"foo@bar.com" "foo@baz.com"}
           :phone ["01234" "5678"]
           :stuff "blah"}

        ;; check all expected records are created
        _ @(u/upsert* tu/*model-session* m r {})
        f-r (tu/fetch-record :delete_mixed_lookup_test [:org_id :id] [org-id id])
        f-nick (tu/fetch-record :delete_mixed_lookup_test_by_nick
                                [:org_id :nick] [org-id "foo"])
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
        f2-email-bar (tu/fetch-record :delete_mixed_lookup_test_by_email
                                      :email "foo@bar.com")
        f2-email-baz (tu/fetch-record :delete_mixed_lookup_test_by_email
                                      :email "foo@baz.com")
        f2-phone-0 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                    :phone "01234")
        f2-phone-5 (tu/fetch-record :delete_mixed_lookup_test_by_phone
                                    :phone "5678")]

    (is (= f-r r))
    (is (= f-nick r))
    (is (= f-email-bar {:org_id org-id :email "foo@bar.com" :id id}))
    (is (= f-email-baz {:org_id org-id :email "foo@baz.com" :id id}))
    (is (= f-phone-0 {:org_id org-id :phone "01234" :id id}))
    (is (= f-phone-5 {:org_id org-id :phone "5678" :id id}))

    (is (= f2-r nil))
    (is (= f2-nick nil))
    (is (= f2-email-bar nil))
    (is (= f2-email-baz nil))
    (is (= f2-phone-0 nil))
    (is (= f2-phone-5 nil))))
