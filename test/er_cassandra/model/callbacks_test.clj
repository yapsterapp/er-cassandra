(ns er-cassandra.model.callbacks-test
  (:require
   [clojure.string :as string]
   [clojure.test :as test :refer [deftest is are use-fixtures testing]]
   [er-cassandra.model.callbacks :as sut]
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback
            -before-save]]
   [er-cassandra.model.types :as t]
   [manifold.deferred :as deferred]
   [schema.test :as st]))

(use-fixtures :once st/validate-schemas)

(deftest test-run-callbacks
  (let [s nil
        m (t/create-entity
           {:primary-table {:name :foos :key [:id]}})
        update-callbacks (fn [m cbs]
                           (assoc-in m [:callbacks :before-save] cbs))
        r {:id 1 :name "bar"}]
    (let [m (update-callbacks m [identity])]
      (testing "identity"
        (is (= r @(sut/run-save-callbacks s m :before-save nil r {})))))
    (testing "callback sequence"
      (let [m (update-callbacks m [#(assoc % :name "baz")
                                   #(update % :name string/upper-case)])
            exp {:id 1 :name "BAZ"}]
        (is (= exp @(sut/run-save-callbacks s m :before-save nil r {})))))
    (testing "callback deferred"
      (let [m (update-callbacks m [(reify ICallback
                                     (-before-save [_ entity old-record record opts]
                                       (deferred/success-deferred
                                         (update record :name string/upper-case))))])
            exp {:id 1 :name "BAR"}]
        (is (= exp @(sut/run-save-callbacks s m :before-save nil r {})))))
    (testing "callback error")))

(deftest create-protect-columns-callback-test
  (testing "nil's protected columns in a new record"
    (let [s nil
          m (t/create-entity {:primary-table {:name :foos :key [:id]}
                              :callbacks {:before-save
                                          [(sut/create-protect-columns-callback
                                            :update-bar?
                                            :bar)]}})
          r @(sut/run-save-callbacks
              s
              m
              :before-save
              nil
              {:id 1000 :bar 100}
              nil)]

      (is (= r
             {:id 1000 :bar nil}))))

  (testing "reverts protected columns to previous values in an updated record"
    (let [s nil
          m (t/create-entity {:primary-table {:name :foos :key [:id]}
                              :callbacks {:before-save
                                          [(sut/create-protect-columns-callback
                                            :update-bar?
                                            :bar)]}})
          r @(sut/run-save-callbacks
              s
              m
              :before-save
              {:id 1000 :bar 1}
              {:id 1000 :bar 100}
              nil)]

      (is (= r
             {:id 1000 :bar 1}))))

  (testing "lets protected column updates through with the right additional col"
    (let [s nil
          m (t/create-entity {:primary-table {:name :foos :key [:id]}
                              :callbacks {:before-save
                                          [(sut/create-protect-columns-callback
                                            :update-bar?
                                            :bar)]}})
          r @(sut/run-save-callbacks
              s
              m
              :before-save
              {:id 1000 :bar 1}
              {:id 1000 :bar 100 :update-bar? true}
              nil)]

      (is (= r
             {:id 1000 :bar 100}))))

  (testing "lets protected column updates through with ::skip-protect"
    (let [s nil
          m (t/create-entity {:primary-table {:name :foos :key [:id]}
                              :callbacks {:before-save
                                          [(sut/create-protect-columns-callback
                                            :update-bar?
                                            :bar)]}})
          r @(sut/run-save-callbacks
              s
              m
              :before-save
              {:id 1000 :bar 1}
              {:id 1000 :bar 100 :update-bar? true}
              {::sut/skip-protect true})]

      (is (= r
             {:id 1000 :bar 100}))))

  (testing "doesn't do anything if the col isn't specified in the new-record"
    (let [s nil
          m (t/create-entity {:primary-table {:name :foos :key [:id]}
                              :callbacks {:before-save
                                          [(sut/create-protect-columns-callback
                                            :update-bar?
                                            :bar)]}})
          r @(sut/run-save-callbacks
              s
              m
              :before-save
              {:id 1000 :bar 1}
              {:id 1000}
              {::sut/skip-protect true})]

      (is (= r
             {:id 1000})))))
