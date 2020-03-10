(ns er-cassandra.model.callbacks.timestamp-boolean-callback-test
  (:require
   [clj-time.coerce :as time.coerce]
   [clj-time.core :as time]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [er-cassandra.model.callbacks.protocol :refer [-before-save]]
   [er-cassandra.model.callbacks.timestamp-boolean-callback :as sut]
   [schema.test :as st]))

(use-fixtures :once st/validate-schemas)

(deftest timestamp-boolean-callback-test
  (let [cb (sut/timestamp-boolean-callback :flag :timestamp)]
    (testing "returns record untouched if bool col is missing"
      (let [dummy-record {:id 1}]
        (is (= dummy-record
               (-before-save cb nil dummy-record dummy-record {})))))
    (testing "sets timestamp when not set and bool is truthy"
      (let [flagged-record {:id 1
                            :flag true}
            ts-before-test (time/now)
            {r-ts :timestamp
             :as r} (-before-save cb nil flagged-record flagged-record {})
            ts-after-test (time/now)]
        (is (time/within?
             ts-before-test
             ts-after-test
             (time.coerce/from-date r-ts)))))
    (testing "doesn't change timestamp when set and bool is truthy"
      (let [timestamped-record {:id 1
                                :flag true
                                :timestamp (time.coerce/to-date (time/now))}]
        (is (= timestamped-record
               (-before-save cb nil timestamped-record timestamped-record {})))))
    (testing "clears timestamp when set and bool is not truthy"
      (let [timestamped-record {:id 1
                                :flag false
                                :timestamp (time.coerce/to-date (time/now))}]
        (is (= (assoc
                timestamped-record
                :timestamp nil)
               (-before-save cb nil timestamped-record timestamped-record {})))))))
