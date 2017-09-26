(ns er-cassandra.concatenated-key-test
  (:require
   [schema.test :as st]
   [clojure.test :as t :refer [deftest is testing]]
   [er-cassandra.util.time :as cut]
   [er-cassandra.concatenated-key :as sut]))

(t/use-fixtures :once st/validate-schemas)

(deftest segment-encoding-test
  (testing "string segment"
    (is (= "foo" (sut/-str-rep "foo"))))

  (testing "boolean segment"
    (is (= "true" (sut/-str-rep Boolean/TRUE)))
    (is (= "false" (sut/-str-rep Boolean/FALSE))))

  (testing "inst segment"
    (let [t (java.util.Date.)]
      (is (= (cut/unparse-timestamp-utc-millis t)
             (sut/-str-rep t))))))

(deftest concatenate-keys-test
  (testing "all cols supplied"
    (is (= "one/two" (sut/concatenate-keys ["one" "two"]))))
  (testing "some nils"
    (is (= nil (sut/concatenate-keys ["one" nil]))))
  (testing "booleans"
    (is (= "false/two" (sut/concatenate-keys [false, "two"])))
    (is (= "true/two" (sut/concatenate-keys [true, "two"])))
    (is (= "false/true/false" (sut/concatenate-keys [false true false]))))
  (testing "custom separator"
    (is (= "one:two" (sut/concatenate-keys ":" ["one" "two"]))))

  (testing "segment-encodings"
    (let [t (java.util.Date.)]
      (is (= (str "foo/true/" (cut/unparse-timestamp-utc-millis t)))
          (sut/concatenate-keys ["foo" Boolean/TRUE t])))))
