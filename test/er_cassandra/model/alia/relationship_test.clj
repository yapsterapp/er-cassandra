(ns er-cassandra.model.alia.relationship-test
  (:require
   [er-cassandra.model.util.test :as tu :refer [fetch-record insert-record upsert-instance]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [er-cassandra.record :as r]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.relationship :as rel]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-relationship
  []
  (tu/create-table :simple_relationship_test
                   "(id timeuuid primary key, nick text)")
  (tu/create-table :simple_relationship_test_target
                   "(id timeuuid primary key, parent_id uuid, nick text)")
  (tu/create-table :simple_relationship_test_target_by_parent_id
                   "(id timeuuid, parent_id uuid primary key, nick text)")
  (let [target (t/create-entity
                {:primary-table {:name :simple_relationship_test_target
                                 :key [:id]}
                 :secondary-tables [{:name :simple_relationship_test_target_by_parent_id
                                     :key [:parent_id]}]})
        source (t/create-entity
                {:primary-table {:name :simple_relationship_test
                                 :key [:id]}
                 :denorm-targets {:test {:target target
                                         :denormalize {:nick :nick}
                                         :cascade :none
                                         :foreign-key [:parent_id]}}})]
    [source target]))

(deftest update-relationship-test
  (let [[s t] (create-simple-relationship)
        [sid tid] [(uuid/v1) (uuid/v1)]
        sr {:id sid :nick "foo"}
        _ (insert-record :simple_relationship_test sr)
        _ (upsert-instance t {:id tid :parent_id sid :nick "bar"})

        resp @(rel/denormalize tu/*model-session* s sr :upsert {})
        potr (fetch-record :simple_relationship_test_target [:id] [tid])
        potri (fetch-record :simple_relationship_test_target_by_parent_id [:parent_id] [sid])
        ]
    (is (= [[:test [:ok]]] resp))
    (is (= "foo" (:nick potr)))
    (is (= "foo" (:nick potri)))))
