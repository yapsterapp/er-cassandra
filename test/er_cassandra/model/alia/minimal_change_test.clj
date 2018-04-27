(ns er-cassandra.model.alia.minimal-change-test
  (:require
   [clojure.test :as t :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clojure.string :as str]
   [er-cassandra.model.alia.minimal-change :as sut]))

(deftest minimal-change-for-table-test
  (testing "removes nils for new record"
    (is (= {:id 10}
           (sut/minimal-change-for-table
            {:key [:id]}
            nil
            {:id 10 :foo nil}))))
  (testing "returns nil if no non-key changes"
    (is (= nil
           (sut/minimal-change-for-table
            {:key [:id]}
            {:id 10 :foo 10}
            {:id 10 :foo 10}))))
  (testing "returns changes"
    (is (= {:id 10 :foo 20}
           (sut/minimal-change-for-table
            {:key [:id]}
            {:id 10 :foo 10}
            {:id 10 :foo 20}))))
  (testing "removes nil columns in updated record"
    (is (= {:id 10 :foo 20}
           (sut/minimal-change-for-table
            {:key [:id]}
            {:id 10 :foo 10 :bar nil}
            {:id 10 :foo 20 :bar nil}))))
  (testing "doesn't remove newly nil'd column"
    (is (= {:id 10 :foo 20 :bar nil}
           (sut/minimal-change-for-table
            {:key [:id]}
            {:id 10 :foo 10 :bar 10}
            {:id 10 :foo 20 :bar nil}))))
  (testing "doesn't remove newly set column"
    (is (= {:id 10 :foo 20 :bar 10}
           (sut/minimal-change-for-table
            {:key [:id]}
            {:id 10 :foo 10 :bar nil}
            {:id 10 :foo 20 :bar 10}))))
  (testing "works with a partition key"
    (is (= {:org_id 0 :id 10 :foo 20 :bar 10}
           (sut/minimal-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo 10 :bar nil}
            {:org_id 0 :id 10 :foo 20 :bar 10})))))

(deftest avoid-tombstone-change-for-table-test
  (testing "removes nil column in new record"
    (is (= {:id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [:id]}
            nil
            {:id 10 :foo nil}))))
  (testing "removes nil column in updated record"
    (is (= {:id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [:id]}
            {:id 10 :foo nil}
            {:id 10 :foo nil}))))
  (testing "doesn't remove newly nil'd column"
    (is (= {:id 10 :foo nil}
           (sut/avoid-tombstone-change-for-table
            {:key [:id]}
            {:id 10 :foo 10}
            {:id 10 :foo nil}))))
  (testing "doesn't remove newly set column"
    (is (= {:id 10 :foo 10}
           (sut/avoid-tombstone-change-for-table
            {:key [:id]}
            {:id 10 :foo nil}
            {:id 10 :foo 10}))))
  (testing "works with partition key"
    (is (= {:org_id 0 :id 10 :foo nil}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo 10}
            {:org_id 0 :id 10 :foo nil}))))

  (testing "removes unmodified collections"
    (is (= {:org_id 0 :id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo #{:foofoo}}
            {:org_id 0 :id 10 :foo #{:foofoo}})))
    (is (= {:org_id 0 :id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo [:foofoo]}
            {:org_id 0 :id 10 :foo [:foofoo]})))
    (is (= {:org_id 0 :id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo {:foofoo 10}}
            {:org_id 0 :id 10 :foo {:foofoo 10}})))
    (is (= {:org_id 0 :id 10}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo '(:foofoo)}
            {:org_id 0 :id 10 :foo '(:foofoo)}))))

  (testing "doesn't remove modified collections"
    (is (= {:org_id 0 :id 10 :foo #{:foofoo}}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo #{}}
            {:org_id 0 :id 10 :foo #{:foofoo}})))
    (is (= {:org_id 0 :id 10 :foo [:foofoo]}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo []}
            {:org_id 0 :id 10 :foo [:foofoo]})))
    (is (= {:org_id 0 :id 10 :foo {:foofoo 10}}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo {}}
            {:org_id 0 :id 10 :foo {:foofoo 10}})))
    (is (= {:org_id 0 :id 10 :foo '(:foofoo)}
           (sut/avoid-tombstone-change-for-table
            {:key [[:org_id] :id]}
            {:org_id 0 :id 10 :foo '()}
            {:org_id 0 :id 10 :foo '(:foofoo)})))))
