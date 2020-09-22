(ns er-cassandra.model.callbacks.distinct-vector-callback-test
  (:require
   [er-cassandra.model.callbacks.distinct-vector-callback :as sut]
   [clojure.test :refer [deftest is are testing]]))


(deftest create-distinct-vector-callback-test
  (let [cb (sut/create-distinct-vector-callback :foo)]
    (testing "does nothing if col not present"
      (is (= {:bar 10} (cb {:bar 10}))))
    (testing "distincts the col value if present"
      (is (= {:bar 10 :foo [1 2 3]} (cb {:bar 10 :foo [1 1 2 1 2 2 3 1]}))))
    (testing "removes any nils from the col value"
      (is (= {:bar 10 :foo [1 2 3]} (cb {:bar 10 :foo [1 1 2 1 nil 2 2 3 1 nil]}))))
    (testing "returns an empty value if only nils"
      (is (= {:bar 10 :foo []} (cb {:bar 10 :foo [nil nil nil]}))))))
