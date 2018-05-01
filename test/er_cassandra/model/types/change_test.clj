(ns er-cassandra.model.types.change-test
  (:require
   [clojure.test :as t :refer [deftest is are use-fixtures testing]]
   [clojure.string :as string]
   [manifold.deferred :as deferred]
   [schema.test :as st]
   [er-cassandra.model.types :as m.t]
   [er-cassandra.model.types.change :as sut])
  (:import
   [clojure.lang ExceptionInfo]))

(use-fixtures :once st/validate-schemas)

(defn foos-all-table-types-entity
  []
  (m.t/create-entity
   {:primary-table {:name :foos :key [:id]}
    :secondary-tables [{:name :foos_by_bar
                        :key [[:bar] :id]}
                       {:name :foos_by_box_and_flag
                        :key [[:box :flag] :id]}
                       {:name :foos_by_stuff_and_wotsit
                        :key [[:stuff :wotsit] :id]
                        :view? true}]
    :lookup-tables [{:name :foos_by_baz
                     :key [[:baz] :id]
                     :with-columns [:bar :bloop]}
                    {:name :foos_by_thing
                     :key [[:thing] :id]
                     :with-columns :all}
                    {:name :foos_by_country
                     :key [[:country] :id]}
                    {:name :foos_by_argh
                     :key [[:argh] :id]
                     :view? true}]
    :unique-key-tables [{:name :foos_by_email
                         :key [:email]
                         :with-columns [:bar :meep]}
                        {:name :foos_by_phone
                         :key [:phone]
                         :with-columns :all}]}))

(deftest additional-cols-for-table-test
  (let [{pt :primary-table
         [foos-by-email
          :as ukts] :unique-key-tables
         [foos-by-bar
          foos-by-stuff-and-wotsit
          :as sts] :secondary-tables
         [foos-by-baz
          :as lts] :lookup-tables
         :as m} (foos-all-table-types-entity)]
    (testing "for primary table"
      (is (= :all
             (sut/additional-cols-for-table m pt))))
    (testing "for secondary table"
      (is (= :all
             (sut/additional-cols-for-table m foos-by-bar))))
    (testing "for uniquekey table"
      (is (= [:bar :meep]
             (sut/additional-cols-for-table m foos-by-email))))
    (testing "for lookup table"
      (is (= [:bar :bloop]
             (sut/additional-cols-for-table m foos-by-baz))))
    (testing "throws for unknown table type"
      (is (thrown?
           Exception
           (sut/additional-cols-for-table
            m
            {:name :foos_by_whateva
             :key [[:whateva] :id]}))))))

(deftest change-contribution-test
  (let [{pt :primary-table
         [foos-by-email
          foos-by-phone
          :as ukts] :unique-key-tables
         [foos-by-bar
          foos-by-box-and-flag
          foos-by-stuff-and-wotsit
          :as sts] :secondary-tables
         [foos-by-baz
          foos-by-thing
          :as lts] :lookup-tables
         :as m} (foos-all-table-types-entity)]

    ;; primary table

    (testing "primary table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :bar 10 :bloop 50 :blargh 12 :blap 23}
              {:id 10 :bar 10 :bloop 50 :blargh 12 :blap 23}
              #{}
              pt))))

    (testing "primary table includes all changed cols (except uberkey)"
      (is (= #{:bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:bar :bloop :blargh}
               pt)))))

    ;; secondary table

    (testing "secondary table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
              #{}
              foos-by-bar))))

    (testing "secondary table returns nil if old and new keys have nil cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :box nil :flag 51  :blargh 14 :blap 23}
              {:id 10 :box 10  :flag nil :blargh 15 :blap 23}
              #{:box :flag :blargh}
              foos-by-box-and-flag))))

    (testing "secondary table includes all cols if key changes"
      (is (= #{:bar :bloop :blargh :blap}
             (set
              (sut/change-contribution
               m
               {:id 10 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:bar :bloop :blargh}
               foos-by-bar)))))

    (testing "secondary table includes all changed cols if key doesn't change"
      (is (= #{:bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :bar 10 :bloop 50 :blargh 12 :blap 23}
               {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:bloop :blargh}
               foos-by-bar)))))

    ;; lookup table

    (testing "lookup table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
              #{}
              foos-by-baz))))

    (testing "lookup table returns nil if old and new key have nil cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :baz nil :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :baz nil :bar 20 :bloop 99 :blargh 14 :blap 23}
              #{:bar :bloop}
              foos-by-baz))))

    (testing "lookup table includes all with-cols if key changes"
      (is (= #{:baz :bar :bloop}
             (set
              (sut/change-contribution
               m
               {:id 10 :baz 0 :bar 10  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :baz 1 :bar 10 :bloop 50  :blargh 14 :blap 23}
               #{:baz :blargh}
               foos-by-baz)))))

    (testing "lookup table includes changed with-cols if key doesn't change"
      (is (= #{:baz :bar}
             (set
              (sut/change-contribution
               m
               {:id 10 :baz 1 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :baz 1 :bar 10 :bloop 50 :blargh 14 :blap 23}
               #{:bar :blargh}
               foos-by-baz)))))

    (testing "lookup table with-columns :all includes all cols if key changes"
      (is (= #{:thing :baz :bar :bloop :blargh :blap}
             (set
              (sut/change-contribution
               m
               {:id 10 :thing 10 :baz 0 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :thing 11 :baz 0 :bar 5  :bloop 50 :blargh 12 :blap 23}
               #{:thing}
               foos-by-thing)))))

    (testing "lookup table with-columns :all includes all changed cols if key doesn't change"
      (is (= #{:thing :bar :bloop}
             (set
              (sut/change-contribution
               m
               {:id 10 :thing 10 :baz 0 :bar 5   :bloop 50 :blargh 12 :blap 23}
               {:id 10 :thing 10 :baz 0 :bar 10  :bloop 99 :blargh 12 :blap 23}
               #{:bar :bloop}
               foos-by-thing)))))

    ;; uniquekey table

    (testing "uniquekey table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :email "foo@bar.com" :bar 10}
              {:id 10 :email "foo@bar.com" :bar 10}
              #{}
              foos-by-email))))

    (testing "uniquekey table returns nil if old and new keys have nil cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :email nil :bar 10 :meep 10}
              {:id 10 :email nil :bar 20 :meep 20}
              #{:bar :meep}
              foos-by-email))))

    (testing "uniquekey table includes all with-cols if key changes"
      (is (= #{:email :bar :meep}
             (set
              (sut/change-contribution
               m
               {:id 10 :email "foo@bar.com" :bar 10 :meep 10 :bloop 50 :blargh 12 :blap 23}
               {:id 10 :email "foo@baz.com" :bar 10 :meep 10 :bloop 51 :blargh 14 :blap 23}
               #{:email :bloop :blargh}
               foos-by-email)))))

    (testing "uniquekey table includes changed with-cols if key doesn't change"
      (is (= #{:email :bar}
             (set
              (sut/change-contribution
               m
               {:id 10 :email "foo@bar.com" :bar 5  :meep 10 :bloop 50 :blargh 12 :blap 23}
               {:id 10 :email "foo@bar.com" :bar 10 :meep 10 :bloop 51 :blargh 14 :blap 23}
               #{:bar :bloop :blargh}
               foos-by-email)))))

    (testing "uniquekey table :with-columns :all includes all cols if key changes"
      (is (= #{:phone :bar :bloop :blargh :blap}
             (set
              (sut/change-contribution
               m
               {:id 10 :phone "123456789" :bar 10 :bloop 50 :blargh 12 :blap 23}
               {:id 10 :phone "987654321" :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:phone :bloop :blargh}
               foos-by-phone)))))

    (testing "uniquekey table :with-columns :all includes changed cols if key doesn't change"
      (is (= #{:phone :bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :phone "123456789" :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :phone "123456789" :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:bar :bloop :blargh}
               foos-by-phone)))))
    ))

(deftest minimal-change-cols-test
  (let [{pt :primary-table
         [foos-by-email
          :as ukts] :unique-key-tables
         [foos-by-bar
          foos-by-stuff-and-wotsit
          :as sts] :secondary-tables
         [foos-by-baz
          :as lts] :lookup-tables
         :as m} (foos-all-table-types-entity)]

    (testing "throws on missing new-record uberkey cols"
      (let [{ex-tag :tag
             :as exd} (try
                        (sut/minimal-change-cols
                         m
                         nil
                         {:foo 30})
                        (catch ExceptionInfo x
                          (ex-data x)))]
        (is (= ::sut/incomplete-uberkey ex-tag))))

    (testing "throws on missing old-record uberkey cols"
      (let [{ex-tag :tag
             :as exd} (try
                        (sut/minimal-change-cols
                         m
                         {:foo 20}
                         {:id 10 :foo 30})
                        (catch ExceptionInfo x
                          (ex-data x)))]
        (is (= ::sut/incomplete-uberkey ex-tag))))

    (testing "throws on missing old-record cols"
      (let [{ex-tag :tag
             :as exd} (try
                        (sut/minimal-change-cols
                         m
                         {:id 10 :foo 20}
                         {:id 10 :foo 30 :bar 100})
                        (catch ExceptionInfo x
                          (ex-data x)))]
        (is (= ::sut/incomplete-old-record ex-tag))))

    (testing "throws on attempt to change uberkey"
      (let [{ex-tag :tag
             :as exd} (try
                        (sut/minimal-change-cols
                         m
                         {:id 10 :foo 20}
                         {:id 11 :foo 30})
                        (catch ExceptionInfo x
                          (ex-data x)))]
        (is (= ::sut/uberkey-changed ex-tag))))

    (testing "returns nil if there are no changes"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :foo 20}
                 {:id 10 :foo 20})]
        (is (= nil mcc))))

    (testing "combines change contributions for non-key col not specifically denormed"
      ;; should contain key cols from
      ;; - primary table
      ;; - non-view secondary tables
      ;; - non-view lookups or uniquekeys with :with-columns :all
      ;; - non-view lookups or uniquekeys with :with-columns :foo
      ;; - any other changed cols
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :box 10 :flag 10 :baz 100 :foo 20}
                 {:id 10 :box 10 :flag 10 :baz 100 :foo 30})]
        (is (= #{:id :box :flag :foo} mcc))))

    (testing "combines change contributions for non-key col specifically denormed"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :bar 20 :baz 30 :country "uk" :email "foo@bar.com" :bloop 10}
                 {:id 10 :bar 20 :baz 30 :country "uk" :email "foo@bar.com" :bloop 20})]
        (is (= #{:id :bar :baz :bloop} mcc))))

    (testing "combines change contributions for secondary-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :box 20 :flag 10}
                 {:id 10 :box 30 :flag 10})]
        (is (= #{:id :box :flag} mcc))))

    (testing "combines change contributions for lookup-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :baz 20 :bar 10 :bloop 10}
                 {:id 10 :baz 30 :bar 10 :bloop 10})]
        (is (= #{:id :baz :bar :bloop} mcc))))

    (testing "combines change contributions for uniquekey-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :email "foo@bar.com" :bar 10 :meep 10}
                 {:id 10 :email "foo@baz.com" :bar 10 :meep 10})]
        (is (= #{:id :email :bar :meep} mcc))))

    (testing "no contribution from keys with nils in old-record and new-record"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :baz nil :bar 100 :bloop 100}
                 {:id 10 :baz nil :bar 100 :bloop 0})]
        (is (= #{:id :bar :bloop} mcc))))
    ))

(deftest minimal-change-test
  (let [{pt :primary-table
         [foos-by-email
          :as ukts] :unique-key-tables
         [foos-by-bar
          foos-by-stuff-and-wotsit
          :as sts] :secondary-tables
         [foos-by-baz
          :as lts] :lookup-tables
         :as m} (foos-all-table-types-entity)]

    ;; (testing "throws if all of the minimal-change-columns are not all present"
    ;;   (let [{x-tag :tag
    ;;          x-value :value
    ;;          :as exd} (try
    ;;                     (sut/minimal-change
    ;;                      m
    ;;                      {:id 10 :bar 10 :box 20 :flag 10 :thing 10}
    ;;                      {:id 10 :bar 10 :box 30 :flag 10 :thing 10})
    ;;                     (catch ExceptionInfo x
    ;;                       (ex-data x)))]
    ;;     (is (= ::sut/missing-columns x-tag))
    ;;     (is (= #{:phone} (:missing-cols x-value)))))

    ;; (testing "returns nil if there is no change"
    ;;   (let [mc (sut/minimal-change
    ;;             m
    ;;             {:id 10 :bar 10 :box 20 :flag 10 :thing 10 :phone "123"}
    ;;             {:id 10 :bar 10 :box 20 :flag 10 :thing 10 :phone "123"})]
    ;;     (is (= mc
    ;;            nil))))

    ;; (testing "selects columns for minimal change"
    ;;   (let [mc (sut/minimal-change
    ;;             m
    ;;             {:id 10 :bar 10 :box 20 :flag 10 :thing 10 :phone "123"}
    ;;             {:id 10 :bar 10 :box 30 :flag 10 :thing 10 :phone "123"})]
    ;;     (is (= mc
    ;;            {:id 10 :bar 10 :box 30 :flag 10 :thing 10 :phone "123"}))))

    ;; (testing "tombstone prevention - no unnecessary nils for a new record"
    ;;   (let [mc (sut/minimal-change
    ;;             m
    ;;             nil
    ;;             {:id 10
    ;;              :bar nil
    ;;              :box 10
    ;;              :flag 20
    ;;              :thing nil
    ;;              :phone nil
    ;;              :flop nil
    ;;              :ding 10})]
    ;;     (is (= mc
    ;;            {:id 10
    ;;             :box 10
    ;;             :flag 20
    ;;             :ding 10}))))

    ;; (testing "tombstone prevention - no unnecessary nils for an updated record"
    ;;   (let [mc (sut/minimal-change
    ;;             m
    ;;             nil
    ;;             {:id 10
    ;;              :bar nil
    ;;              :box nil
    ;;              :flag nil
    ;;              :thing nil
    ;;              :phone nil
    ;;              :flop nil
    ;;              :ding 10})]
    ;;     (is (= mc
    ;;            {:id 10
    ;;             :bar 10
    ;;             :ding 10}))))
    ))
