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

(def v4-lex-lo-time-v4-lex-hi
  [#uuid "1effaa20-8f88-4f34-be5f-0d81646c015e"
   #uuid "723a6b10-e194-11e7-83df-e038a7a06aab"
   #uuid "9df1d6a2-3383-4b80-8efe-e68fef5f2c36"])

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

  (testing "always compares timeuuids below other uuids"
    (let [[v4lo t v4hi] v4-lex-lo-time-v4-lex-hi]
      (is (< (compare (str v4lo) (str t)) 0))
      (is (> (sut/-compare v4lo t) 0))

      (is (< (compare (str t) (str v4hi)) 0))
      (is (< (sut/-compare t v4hi) 0))))

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
