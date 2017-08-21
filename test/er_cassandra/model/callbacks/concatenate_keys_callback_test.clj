(ns er-cassandra.model.callbacks.concatenate-keys-callback-test
  (:require
   [clojure.test :as t :refer [deftest is are use-fixtures testing]]
   [schema.test :as st]
   [er-cassandra.model.callbacks.concatenate-keys-callback :as cb]))

(use-fixtures :once st/validate-schemas)

(deftest concatenate-keys-test
  (testing "all cols supplied"
    (is (= "one/two" (cb/concatenate-keys ["one" "two"]))))
  (testing "some nils"
    (is (= nil (cb/concatenate-keys ["one" nil]))))
  (testing "custom separator"
    (is (= "one:two" (cb/concatenate-keys ":" ["one" "two"])))))

(deftest concatenate-keys-callback-test
  (testing "all cols present"
    (let [cb (cb/concatenate-keys-callback :foobar [:foo :bar])]
      (is (= {:foobar "one/two"
              :foo "one"
              :bar "two"}
             (cb {:foo "one" :bar "two"})))))

  (testing "some nil cols"
    (let [cb (cb/concatenate-keys-callback :foobar [:foo :bar])]
      (is (= {:foobar nil
              :foo "one"
              :bar nil}
             (cb {:foo "one" :bar nil})))))

  (testing "custom separator"
    (let [cb (cb/concatenate-keys-callback :foobar ":" [:foo :bar])]
      (is (= {:foobar "one:two"
              :foo "one"
              :bar "two"}
             (cb {:foo "one" :bar "two"}))))))
