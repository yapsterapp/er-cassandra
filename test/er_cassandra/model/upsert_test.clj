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
   [er-cassandra.session :as c.session]
   [er-cassandra.model.model-session :as ms]
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

(defn create-unique-lookup-secondaries-entity
  []
  (tu/create-table
   :mups_unique_lookup_secondaries_test
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, thing text, title text, tag set<text>, dept list<text>, stuff text, primary key (org_id, id))")

  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_nick
   "(nick text, org_id timeuuid, id timeuuid, primary key (org_id, nick))")
  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_email
   "(email text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_phone
   "(phone text primary key, org_id timeuuid, id timeuuid)")

  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_thing
   "(org_id timeuuid, id timeuuid, nick text, email set<text>, phone list<text>, thing text, title text, tag set<text>, dept list<text>, stuff text, primary key (org_id, thing))")

  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_title
   "(title text, org_id timeuuid, id timeuuid, primary key (org_id, title))")
  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_tag
   "(tag text primary key, org_id timeuuid, id timeuuid)")
  (tu/create-table
   :mups_unique_lookup_secondaries_test_by_dept
   "(dept text primary key, org_id timeuuid, id timeuuid)")

  (t/create-entity
   {:primary-table {:name :mups_unique_lookup_secondaries_test :key [:org_id :id]}
    :unique-key-tables [{:name :mups_unique_lookup_secondaries_test_by_nick
                         :key [:org_id :nick]}
                        {:name :mups_unique_lookup_secondaries_test_by_email
                         :key [:email]
                         :collections {:email :set}}
                        {:name :mups_unique_lookup_secondaries_test_by_phone
                         :key [:phone]
                         :collections {:phone :list}}]
    :secondary-tables [{:name :mups_unique_lookup_secondaries_test_by_thing
                        :key [:org_id :thing]}]
    :lookup-tables [{:name :mups_unique_lookup_secondaries_test_by_title
                         :key [:org_id :title]}
                    {:name :mups_unique_lookup_secondaries_test_by_tag
                     :key [:tag]
                     :collections {:tag :set}}
                    {:name :mups_unique_lookup_secondaries_test_by_dept
                     :key [:dept]
                     :collections {:dept :list}}]}))

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
                    (stream/realize-each)
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

(defn match-statement
  [stmt stmts]
  (some
   (fn [st]
     (= (select-keys st (keys stmt))
        stmt))
   stmts))

(deftest minimal-upsert-test
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

    (testing "insert with minimal change does the insert"
      (let [_ (c.session/reset-spy-log tu/*model-session*)
            r-s @(m/change
                  tu/*model-session*
                  m
                  nil
                  record
                  {::t/minimal-change true})
            stmts (c.session/spy-log tu/*model-session*)]

        ;; check all the index records were created correctly
        (is (= record
               (fetch-record :mups_unique_lookup_secondaries_test
                             [:org_id :id] [org-id id])))
        (is (= (select-keys record [:org_id :id :nick])
               (fetch-record :mups_unique_lookup_secondaries_test_by_nick
                             [:org_id :nick] [org-id "foo"])))
        (is (= (merge {:email "foo@bar.com"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_email
                             [:email] ["foo@bar.com"])))
        (is (= (merge {:email "foo@baz.com"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_email
                             [:email] ["foo@baz.com"])))
        (is (= (merge {:phone "123"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_phone
                             [:phone] ["123"])))
        (is (= (merge {:phone "456"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_phone
                             [:phone] ["456"])))
        (is (= record
               (fetch-record :mups_unique_lookup_secondaries_test_by_thing
                             [:org_id :thing] [org-id "blah"])))
        (is (= (merge {:title "mr"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_title
                             [:org_id :title] [org-id "mr"])))
        (is (= (merge {:tag "quick"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_tag
                             [:tag] ["quick"])))
        (is (= (merge {:tag "slow"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_tag
                             [:tag] ["slow"])))
        (is (= (merge {:dept "hr"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_dept
                             [:dept] ["hr"])))
        (is (= (merge {:dept "dev"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_dept
                             [:dept] ["dev"])))))

    (testing "noop update with minimal change does absolutely nothing"
      (let [_ (c.session/reset-spy-log tu/*model-session*)

            r-s @(m/change
                  tu/*model-session*
                  m
                  record
                  record
                  {::t/minimal-change true})

            stmts (c.session/spy-log tu/*model-session*)]

        (is (= record
               (fetch-record :mups_unique_lookup_secondaries_test
                             [:org_id :id] [org-id id])))
        (is (empty? stmts))))

    (testing "partial update issues optimal update statements"
      (let [_ (c.session/reset-spy-log tu/*model-session*)

            new-record {:org_id org-id
                        :id id

                        :stuff "mobargh"

                        :nick "fooble"
                        :email #{"foo@foo.com" "foo@baz.com"}
                        :phone (sort ["789" "456"])

                        :thing "blarghle"

                        :title "dr"
                        :tag #{"slower" "slow"}
                        :dept (sort ["sales" "dev"])}

            sort-unordered-collections (fn [r]
                                         (-> r
                                             (update :phone sort)
                                             (update :dept sort)))

            ts-before-change (* 1000 (.getTime (java.util.Date.)))
            r-s @(m/change
                  tu/*model-session*
                  m
                  record
                  new-record
                  {::t/minimal-change true
                   ;; force a timestamp we can verify
                   :using {:timestamp ts-before-change}})

            stmts (c.session/spy-log tu/*model-session*)]

        (is (= new-record
               (fetch-record :mups_unique_lookup_secondaries_test
                             [:org_id :id] [org-id id])))

        ;; check all the current index records are there, and all the
        ;; stale index records are gone
        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_nick
                             [:org_id :nick] [org-id "foo"])))
        (is (= (select-keys new-record [:org_id :id :nick])
               (fetch-record :mups_unique_lookup_secondaries_test_by_nick
                             [:org_id :nick] [org-id "fooble"])))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_email
                             [:email] ["foo@bar.com"])))
        (is (= (merge {:email "foo@foo.com"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_email
                             [:email] ["foo@foo.com"])))
        (is (= (merge {:email "foo@baz.com"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_email
                             [:email] ["foo@baz.com"])))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_phone
                             [:phone] ["123"])))
        (is (= (merge {:phone "789"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_phone
                             [:phone] ["789"])))
        (is (= (merge {:phone "456"}
                      (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_phone
                             [:phone] ["456"])))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_thing
                             [:org_id :thing] [org-id "blah"])))
        (is (= new-record
               (sort-unordered-collections
                (fetch-record :mups_unique_lookup_secondaries_test_by_thing
                              [:org_id :thing] [org-id "blarghle"]))))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_title
                             [:org_id :title] [org-id "mr"])))
        (is (= (merge {:title "dr"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_title
                             [:org_id :title] [org-id "dr"])))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_tag
                             [:tag] ["quick"])))
        (is (= (merge {:tag "slower"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_tag
                             [:tag] ["slower"])))
        (is (= (merge {:tag "slow"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_tag
                             [:tag] ["slow"])))

        (is (= nil
               (fetch-record :mups_unique_lookup_secondaries_test_by_dept
                             [:dept] ["hr"])))
        (is (= (merge {:dept "sales"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_dept
                             [:dept] ["sales"])))
        (is (= (merge {:dept "dev"} (select-keys record [:org_id :id]))
               (fetch-record :mups_unique_lookup_secondaries_test_by_dept
                             [:dept] ["dev"])))

        ;; finally check the generated statements include only the
        ;; necessary statements for the requested change


        ;; primary record change without unique-keys
        ;; using update to avoid collection column tombstones
        (is (match-statement
             {:update :mups_unique_lookup_secondaries_test,
              :set-columns [[:stuff "mobargh"]
                            [:title "dr"]
                            [:thing "blarghle"]
                            [:tag [+ #{"slower"}]]
                            [:tag [- #{"quick"}]]
                            [:dept [+ ["sales"]]]
                            [:dept [- ["hr"]]]]
              :where [[:= :org_id org-id]
                      [:= :id id]]
              :using [[:timestamp ts-before-change]]}
             stmts))

        ;; delete old nick lookup LWT
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_nick,
              :columns :*,
              :where
              [[:= :org_id org-id]
               [:= :nick "foo"]],
              :if [[:= :id id]]}
             stmts))

        ;; insert new nick lookup LWT
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_nick,
              :values
              {:org_id org-id,
               :id id,
               :nick "fooble"},
              :if-exists false}
             stmts))

        ;; delete foo@bar.com email LWT
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_email,
              :columns :*,
              :where [[:= :email "foo@bar.com"]],
              :if
              [[:= :org_id org-id]
               [:= :id id]]}
             stmts))

        ;; insert foo@foo.com email LWT
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_email,
              :values
              {:org_id org-id,
               :id id,
               :email "foo@foo.com"},
              :if-exists false}
             stmts))

        ;; delete 123 phone LWT
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_phone,
              :columns :*,
              :where [[:= :phone "123"]],
              :if
              [[:= :org_id org-id]
               [:= :id id]]}
             stmts))

        ;; insert 789 phone LWT
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_phone,
              :values
              {:org_id org-id,
               :id id,
               :phone "789"},
              :if-exists false}
             stmts))

        ;; primary record change unique keys
        (is (match-statement
             {:update :mups_unique_lookup_secondaries_test,
              :set-columns [[:email [+ #{"foo@foo.com"}]]
                            [:email [- #{"foo@bar.com"}]]
                            [:nick "fooble"]
                            [:phone [+ ["789"]]]
                            [:phone [- ["123"]]]]
              :where [[:= :org_id org-id]
                      [:= :id id]]
              :using [[:timestamp ts-before-change]]}
             stmts))

        ;; delete mr title lookup
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_title,
              :columns :*,
              :where
              [[:= :org_id org-id]
               [:= :title "mr"]]}
             stmts))

        ;; insert dr title lookup
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_title,
              :values
              {:org_id org-id,
               :id id,
               :title "dr"}}
             stmts))

        ;; delete quick tag lookup
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_tag,
              :columns :*,
              :where [[:= :tag "quick"]]}
             stmts))

        ;; insert slower tag lookup
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_tag,
              :values
              {:org_id org-id,
               :id id,
               :tag "slower"}}
             stmts))

        ;; delete hr dept lookup
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_dept,
              :columns :*,
              :where [[:= :dept "hr"]]}
             stmts))

        ;; insert sales dept lookup
        (is (match-statement
             {:insert :mups_unique_lookup_secondaries_test_by_dept,
              :values
              {:org_id org-id,
               :id id,
               :dept "sales"}}
             stmts))

        ;; delete blah thing secondary
        (is (match-statement
             {:delete :mups_unique_lookup_secondaries_test_by_thing,
              :columns :*,
              :where
              [[:= :org_id org-id]
               [:= :thing "blah"]]}
             stmts))

        ;; insert blarghle thing secondary
        ;; using update to avoid collection column tombstones
        (is (match-statement
             {:update :mups_unique_lookup_secondaries_test_by_thing,
              :set-columns [[:email [+ #{"foo@foo.com" "foo@baz.com"}]]
                            [:nick "fooble"]
                            [:stuff "mobargh"]
                            [:phone [+ ["789" "456"]]]
                            [:title "dr"]
                            [:id id]
                            [:tag [+ #{"slower" "slow"}]]
                            [:dept [+ ["dev" "sales"]]]]
              :where [[:= :org_id org-id]
                      [:= :thing "blarghle"]]
              :using [[:timestamp ts-before-change]]}
             stmts))))))
