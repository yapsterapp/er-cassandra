(ns er-cassandra.model.alia.minimal-change-test
  (:require
   [clojure.test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clojure.string :as str]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.minimal-change :as sut]))

(defn ->CCD [m] (t/map->CollectionColumnDiff m))

(defn expect-no-change-for-nil-collection-col
  "empty collections should be treated as equivalent to nil and result in no change"
  [fnut empty-col]
  (is (= {:org_id 0 :id 10}
         (fnut
          {:key [[:org_id] :id]}
          {:org_id 0 :id 10 :foo nil}
          {:org_id 0 :id 10 :foo empty-col})))
  (is (= {:org_id 0 :id 10}
         (fnut
          {:key [[:org_id] :id]}
          {:org_id 0 :id 10 :foo empty-col}
          {:org_id 0 :id 10 :foo nil}))))

(defn expect-collection-col-diffs
  [fnut]
  (testing "provides diffs of modified collections"
    (testing "with only empty cols and nils"
      (expect-no-change-for-nil-collection-col fnut #{})
      (expect-no-change-for-nil-collection-col fnut '())
      (expect-no-change-for-nil-collection-col fnut [])
      (expect-no-change-for-nil-collection-col fnut {}))
    (testing "with only added elems"
      (testing "(sets)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection #{} + #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo nil}
                {:org_id 0 :id 10 :foo #{:foofoo}})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection #{} + #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo #{}}
                {:org_id 0 :id 10 :foo #{:foofoo}}))))
      (testing "(lists)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection '() + '(:foofoo)})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo nil}
                {:org_id 0 :id 10 :foo '(:foofoo)})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection '() + '(:foofoo)})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo '()}
                {:org_id 0 :id 10 :foo '(:foofoo)}))))
      (testing "(vectors)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection [] + [:foofoo]})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo nil}
                {:org_id 0 :id 10 :foo [:foofoo]})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection [] + [:foofoo]})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo []}
                {:org_id 0 :id 10 :foo [:foofoo]}))))
      (testing "(maps)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection {} + {:foofoo 10}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo nil}
                {:org_id 0 :id 10 :foo {:foofoo 10}})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection {} + {:foofoo 10}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo {}}
                {:org_id 0 :id 10 :foo {:foofoo 10}})))))
    (testing "with only removed elems"
      (testing "(sets)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection #{} - #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo #{:foofoo}}
                {:org_id 0 :id 10 :foo nil})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection #{} - #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo #{:foofoo}}
                {:org_id 0 :id 10 :foo #{}}))))
      (testing "(lists)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection '() - '(:foofoo)})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo '(:foofoo)}
                {:org_id 0 :id 10 :foo nil})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection '() - '(:foofoo)})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo '(:foofoo)}
                {:org_id 0 :id 10 :foo '()}))))
      (testing "(vectors)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection [] - [:foofoo]})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo [:foofoo]}
                {:org_id 0 :id 10 :foo nil})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection [] - [:foofoo]})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo [:foofoo]}
                {:org_id 0 :id 10 :foo []}))))
      (testing "(maps)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection {} - #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo {:foofoo 10}}
                {:org_id 0 :id 10 :foo nil})))
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection {} - #{:foofoo}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo {:foofoo 10}}
                {:org_id 0 :id 10 :foo {}})))))
    (testing "with added removed and kept elems"
      (testing "(sets)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection #{:barbaz}
                                              + #{:foofoo}
                                              - #{:barbar}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo #{:barbaz :barbar}}
                {:org_id 0 :id 10 :foo #{:barbaz :foofoo}}))))
      (testing "(lists)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection '(:barbaz)
                                              + '(:foofoo)
                                              - '(:barbar)})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo '(:barbaz :barbar)}
                {:org_id 0 :id 10 :foo '(:barbaz :foofoo)}))))
      (testing "(vectors)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection [:barbaz]
                                              + [:foofoo]
                                              - [:barbar]})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo [:barbaz :barbar]}
                {:org_id 0 :id 10 :foo [:barbaz :foofoo]}))))
      (testing "(maps)"
        (is (= {:org_id 0 :id 10 :foo (->CCD {:intersection {:barbaz 50}
                                              + {:foofoo 10
                                                 :bazbaz 30}
                                              - #{:barbar}})}
               (fnut
                {:key [[:org_id] :id]}
                {:org_id 0 :id 10 :foo {:barbaz 50 :bazbaz 20 :barbar 20}}
                {:org_id 0 :id 10 :foo {:barbaz 50 :bazbaz 30 :foofoo 10}})))))))

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
            {:org_id 0 :id 10 :foo 20 :bar 10}))))
  (expect-collection-col-diffs
   sut/minimal-change-for-table))

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

  (expect-collection-col-diffs
   sut/avoid-tombstone-change-for-table))
