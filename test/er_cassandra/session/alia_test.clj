(ns er-cassandra.session.alia-test
  (:require [clojure.test :as test :refer [deftest is are testing use-fixtures]]
            [er-cassandra.session.alia :as sut]))

(deftest tables-to-truncate-test
  (testing "that the correct tables will be excluded from any post-test run truncations"
    (let [expected-tables-to-truncate ["test_table" "test_table_two"]]
      (is (= expected-tables-to-truncate
             (sut/tables-to-truncate ["test_table" "test_table_two"])))
      (is (= expected-tables-to-truncate
             (sut/tables-to-truncate ["test_table" "migrations" "test_table_two"])))
      (is (= expected-tables-to-truncate
             (sut/tables-to-truncate ["schema_migrations" "test_table" "test_table_two"])))
      (is (= expected-tables-to-truncate
             (sut/tables-to-truncate ["test_table" "migrations" "test_table_two" "schema_migrations"]))))))
