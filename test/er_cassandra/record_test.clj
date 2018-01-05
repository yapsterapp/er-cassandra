(ns er-cassandra.record-test
  (:require
   [er-cassandra.util.test :as tu]
   [er-cassandra.session :as s]
   [er-cassandra.session.alia :as alia-session]
   [er-cassandra.record :as r]
   [schema.test :as st]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clj-uuid :as uuid]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-session-fixture))

(deftest insert-select-delete-test
  (let [_ (tu/create-table :stmt_test "(id timeuuid primary key)")

        t (uuid/v1)
        ;; insert with record layer to trigger truncates on session close
        _ @(r/insert
            tu/*session*
            :stmt_test
            {:id t})]
    (testing "simple select-one"
      (is (= {:id t}
             @(r/select-one tu/*session* :stmt_test :id t))))
    (testing "delete"
      (let [_ @(r/delete
                tu/*session*
                :stmt_test
                :id
                t)]
        (is (= nil
               @(r/select-one tu/*session* :stmt_test :id t)))))))


(deftest prepared-insert-select-delete-test
  (let [_ (tu/create-table :stmt_test "(id timeuuid primary key)")

        t (uuid/v1)
        ;; insert with record layer to trigger truncates on session close
        _ @(r/insert
            tu/*session*
            :stmt_test
            {:id t}
            {:prepare? true})]
    (testing "simple select-one"
      (is (= {:id t}
             @(r/select-one tu/*session* :stmt_test :id t {:prepare? true}))))
    (testing "delete"
      (let [_ @(r/delete
                tu/*session*
                :stmt_test
                :id
                t
                {:prepare? true})]
        (is (= nil
               @(r/select-one tu/*session* :stmt_test :id t {:prepare? true})))))))
