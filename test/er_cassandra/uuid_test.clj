(ns er-cassandra.uuid-test
  (:require
   [er-cassandra.uuid :as sut]
   [clojure.test :as t :refer [deftest testing is]]
   [clj-time.coerce :as tc]
   [clj-uuid :as uuid]))

(defn same-time-timeuuids
  []
  (let [t (uuid/v1)
        low (sut/time->start-of-timeuuid t)
        hi (sut/time->end-of-timeuuid t)]
    [low t hi]))

(def contradictory-lex-timeuuids
  [#uuid "723a6b10-e194-11e7-83df-e038a7a06aab" ;; time earlier, lexically later
   #uuid "13e17bf0-f7c2-11e7-bf96-d454f28d853f" ;; time later, lexically earlier
   ])

(deftest -compare-test
  (testing "correctly compares time-lexical contradictory timeuuids"
    (let [[before after] contradictory-lex-timeuuids]
      (is (< (-> before uuid/get-instant tc/to-long)
             (-> after uuid/get-instant tc/to-long)))
      (is (> (compare before after) 0))
      (is (< (sut/-compare before after) 0))
      (is (> (sut/-compare after before) 0))
      (is (= (sut/-compare after after) 0))
      (is (= (sut/-compare before before) 0))))

  (testing "correctly compares same-time timeuuids correctly"
    (let [[low t hi] (same-time-timeuuids)]
      (is (< (sut/-compare low t) 0))
      (is (< (sut/-compare t hi) 0))
      (is (< (sut/-compare low hi) 0))
      (is (> (sut/-compare hi t) 0))
      (is (> (sut/-compare t low) 0))
      (is (> (sut/-compare hi low) 0))))

  (testing "errors when a uuid is compared with another type"
    (let [[low t hi] (same-time-timeuuids)]
      (is (thrown? Exception
                   < (sut/-compare "0" low)))))

  (testing "compares vectors with mixed timeuuids and other things correctly"
    (let [[before after] contradictory-lex-timeuuids]
      (is (< (sut/-compare
              [before]
              [after])
             0))

      (is (< (sut/-compare
              [before before]
              [before after])
             0))

      (is (< (sut/-compare
              ["foo" "bar"]
              ["foo" "foo"])
             0))

      (is (< (sut/-compare
              ["foo" before]
              ["foo" after])
             0))

      (is (< (sut/-compare
              [before "bar"]
              [before "foo"])
             0)))))
