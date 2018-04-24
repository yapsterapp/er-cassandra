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
                     :with-columns [:bar]}
                    {:name :foos_by_thing
                     :key [[:thing] :id]
                     :with-columns :all}
                    {:name :foos_by_argh
                     :key [[:argh] :id]
                     :view? true}]
    :unique-key-tables [{:name :foos_by_email
                         :key [:email]
                         :with-columns [:bar]}
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
      (is (= [:bar]
             (sut/additional-cols-for-table m foos-by-email))))
    (testing "for lookup table"
      (is (= [:bar]
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

    (testing "partial key change throws"
      (let [exd (try
                  (sut/change-contribution
                   m
                   {:id 10 :box "blah" }
                   {:id 10 :box "bloop"}
                   #{:box}
                   foos-by-box-and-flag)
                  (catch ExceptionInfo x
                    (prn "boo")
                    (ex-data x)))]
        (is (= ::sut/partial-key-change
               (:tag exd)))))

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

    (testing "secondary table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
              #{}
              foos-by-bar))))

    (testing "secondary table includes all changed cols"
      (is (= #{:bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:bar :bloop :blargh}
               foos-by-bar)))))

    (testing "lookup table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
              #{}
              foos-by-baz))))

    (testing "lookup table includes only specified changed cols"
      (is (= #{:baz :bar}
             (set
              (sut/change-contribution
               m
               {:id 10 :baz 0 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:baz :bar :bloop :blargh}
               foos-by-baz)))))

    (testing "lookup table with-columns :all includes all changed cols"
      (is (= #{:thing :baz :bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :thing 10 :baz 0 :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :thing 11 :baz 1 :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:thing :baz :bar :bloop :blargh}
               foos-by-thing)))))

    (testing "uniquekey table returns nil if no changed cols"
      (is (= nil
             (sut/change-contribution
              m
              {:id 10 :email "foo@baz.com" :bar 10 :bloop 51 :blargh 14 :blap 23}
              {:id 10 :email "foo@baz.com" :bar 10 :bloop 51 :blargh 14 :blap 23}
              #{}
              foos-by-email))))

    (testing "uniquekey table includes only specified changed cols"
      (is (= #{:email :bar}
             (set
              (sut/change-contribution
               m
               {:id 10 :email "foo@bar.com" :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :email "foo@baz.com" :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:email :bar :bloop :blargh}
               foos-by-email)))))

    (testing "uniquekey table :with-columns :all includes all changed cols"
      (is (= #{:phone :bar :bloop :blargh}
             (set
              (sut/change-contribution
               m
               {:id 10 :phone "123456789" :bar 5  :bloop 50 :blargh 12 :blap 23}
               {:id 10 :phone "987654321" :bar 10 :bloop 51 :blargh 14 :blap 23}
               #{:phone :bar :bloop :blargh}
               foos-by-phone)))))))

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
                         {:id 10 :foo 20}
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

    (testing "combines change contributions for non-key col not specifically denormed"
      ;; should contain key cols from
      ;; - primary table
      ;; - non-view secondary tables
      ;; - non-view lookups or uniquekeys with :with-columns :all
      ;; - non-view lookups or uniquekeys with :with-columns :foo
      ;; - any other changed cols
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :foo 20}
                 {:id 10 :foo 30})]
        (is (= #{:id :bar :box :flag :thing :phone :foo} mcc))))

    (testing "combines change contributions for non-key col specifically denormed"
      ;; should contain key cols from
      ;; - primary table
      ;; - non-view secondary tables
      ;; - non-view lookups or uniquekeys with :with-columns :all
      ;; - non-view lookups or uniquekeys with :with-columns :bar
      ;; - any other changed cols
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :bar 20}
                 {:id 10 :bar 30})]
        (is (= #{:id :bar :box :flag :baz :thing :email :phone} mcc))))

    (testing "combines change contributions for secondary-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :box 20 :flag 10}
                 {:id 10 :box 30 :flag 10})]
        (is (= #{:id :bar :box :flag :thing :phone} mcc))))

    (testing "combines change contributions for lookup-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :baz 20}
                 {:id 10 :baz 30})]
        (is (= #{:id :bar :box :flag :baz :thing :phone} mcc))))

    (testing "combines change contributions for uniquekey-table key col"
      (let [mcc (sut/minimal-change-cols
                 m
                 {:id 10 :email "foo@bar.com"}
                 {:id 10 :email "foo@baz.com"})]
        (is (= #{:id :bar :box :flag :thing :email :phone} mcc))))
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

    (testing "throws if all of the minimal-change-columns are not all present"
      (let [{x-tag :tag
             x-value :value
             :as exd} (try
                        (sut/minimal-change
                         m
                         {:id 10 :bar 10 :box 20 :flag 10 :thing 10}
                         {:id 10 :bar 10 :box 30 :flag 10 :thing 10})
                        (catch ExceptionInfo x
                          (ex-data x)))]
        (is (= ::sut/missing-columns x-tag))
        (is (= #{:phone} (:missing-cols x-value)))))

    (testing "selects columns for minimal change"
      (let [mc (sut/minimal-change
                m
                {:id 10 :bar 10 :box 20 :flag 10 :thing 10 :phone "123"}
                {:id 10 :bar 10 :box 30 :flag 10 :thing 10 :phone "123"})]
        (is (= mc
               {:id 10 :bar 10 :box 30 :flag 10 :thing 10 :phone "123"}))))))
