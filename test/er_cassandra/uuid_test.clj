(ns er-cassandra.uuid-test
  (:require
   [er-cassandra.record :as r]
   [er-cassandra.util.test :as tu]
   [er-cassandra.uuid :as sut]
   [clojure.test :as t :refer [deftest testing is use-fixtures]]
   [clj-time.coerce :as tc]
   [clj-uuid :as uuid]
   [prpr.stream :as stream])
  (:import
   [clojure.lang ExceptionInfo]))

(use-fixtures :each (tu/with-session-fixture))

(defn same-time-timeuuids
  []
  (let [t (uuid/v1)
        low (sut/time->start-of-timeuuid (uuid/get-instant t))
        hi (sut/time->end-of-timeuuid (uuid/get-instant t))]
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
             0))))

  (testing "comparing uuid with non-uuid fails"
    (let [[_ t _] (same-time-timeuuids)
          [r x] (try
                  [(sut/-compare t "bar") nil]
                  (catch Exception x
                    [nil x]))]
      (is (nil? r))
      (is (= "er-cassandra.uuid/cant-compare" (some-> x .getMessage)))
      (is (some? (ex-data x)))
      (is (= {:a t :b "bar"} (ex-data x)))))

  (testing "error during array compare reports whole array"
    (let [[_ t _] (same-time-timeuuids)
          [r x] (try
                  [(sut/-compare [t] ["bar"]) nil]
                  (catch Exception x
                    [nil x]))]
      (is (nil? r))
      (is (= "er-cassandra.uuid/cant-compare" (some-> x .getMessage)))
      (is (some? (ex-data x)))
      (is (= {:a [t] :b ["bar"]} (ex-data x))))))

(deftest -compare-mirrors-cassandra-test
  (let [_ (tu/create-table
           :uuid_test
           (str
            "(test_id uuid, seq int, uuid uuid, version int"
            ", primary key ((test_id), uuid, seq))"))

        test-id (uuid/v1)

        uuids (concat
               contradictory-lex-timeuuids
               [(uuid/v5 uuid/+namespace-url+ "https://www.google.com/")
                (uuid/v5 uuid/+namespace-dns+ "8.8.8.8")]
               v4-lex-lo-time-v4-lex-hi
               [(uuid/v0)]
               [(uuid/v3 uuid/+namespace-dns+ "9.9.9.9")
                (uuid/v3 uuid/+namespace-url+ "https://www.bing.com/")])

        indexed-uuids (map-indexed (fn [i uuid] [i uuid]) uuids)

        _ (doseq [[i uuid] indexed-uuids]
            @(r/insert
              tu/*session*
              :uuid_test
              {:test_id test-id
               :seq i
               :uuid uuid
               :version (uuid/get-version uuid)}))

        cassandra-sorted-uuids (->> (r/select-buffered
                                     tu/*session*
                                     :uuid_test
                                     [:test_id]
                                     [test-id]
                                     {:order-by [[:uuid :asc]]})
                                    deref
                                    (stream/map #(dissoc % :test_id))
                                    (stream/reduce conj [])
                                    deref)

        sorted-uuids (mapv
                      (fn [[i uuid]]
                        {:seq i
                         :uuid uuid
                         :version (uuid/get-version uuid)})
                      (sort-by second sut/-compare indexed-uuids))]
    (testing "comparison order matches Cassandra"
      (is (= cassandra-sorted-uuids
             sorted-uuids)))))
