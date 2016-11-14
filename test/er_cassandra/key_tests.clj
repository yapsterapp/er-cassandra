(ns er-cassandra.key-tests
  (:require [clojure.test :as test :refer [deftest is are testing]]
            [er-cassandra.key :refer [has-key?]]))


(deftest nested-keys
  (are [k expected-val] (= (has-key? k {:x 1 :y 2 :z 3}))
    [:x]      true
    [[:x] :z] true
    [[:x] :A] false
    [[:x :y]] true))
