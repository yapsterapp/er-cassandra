(ns er-cassandra.model.callbacks.timestamp-timeuuid-callback-test
  (:require
   [clj-uuid :as uuid]
   [er-cassandra.model.callbacks.protocol :as cb.proto]
   [er-cassandra.model.callbacks.timestamp-timeuuid-callback :as sut]
   [clojure.test :refer [deftest is are testing]]))

(deftest created-at-timeuuid-callback-test
  (testing "preserves old value if not a new record"
    (let [cb (sut/created-at-timeuuid-callback :timeuuid)]
      (is (= {:timeuuid :foo}
             (cb.proto/-before-save
              cb
              :entity
              {:timeuuid :foo}
              {}
              nil)))))

  (testing "sets the timeuuid col if a new record"
    (let [cb (sut/created-at-timeuuid-callback :timeuuid)
          {r-timeuuid :timeuuid
           :as r} (cb.proto/-before-save
                   cb
                   :entity
                   nil
                   {}
                   nil)]

      (is (some? r-timeuuid))
      (is (uuid/uuid? r-timeuuid))
      (is (= 1 (uuid/get-version r-timeuuid)))))

  (testing "allows override"
    (let [cb (sut/created-at-timeuuid-callback :timeuuid)]
      (is (= {:timeuuid :foo}
             (cb.proto/-before-save
              cb
              :entity
              {:timeuuid :foo}
              {:timeuuid :bar}
              nil))))
    (let [cb (sut/created-at-timeuuid-callback :timeuuid)]
      (is (= {:timeuuid :bar}
             (cb.proto/-before-save
              cb
              :entity
              {:timeuuid :foo}
              {:timeuuid :bar :set-timeuuid? true}
              nil))))
    (let [cb (sut/created-at-timeuuid-callback :timeuuid :update-timeuuid?)]
      (is (= {:timeuuid :bar}
             (cb.proto/-before-save
              cb
              :entity
              {:timeuuid :foo}
              {:timeuuid :bar :update-timeuuid? true}
              nil))))))

(deftest created-at-callback-test
  (testing "does nothing if not a new record"
    (let [cb (sut/created-at-callback :created_at)]
      (is (= {:created_at :foo}
             (cb.proto/-before-save
              cb
              :entity
              {:created_at :foo}
              {}
              nil)))))

  (testing "sets the timeuuid col if a new record"
    (let [cb (sut/created-at-callback :created_at)
          {r-created-at :created_at
           :as r} (cb.proto/-before-save
                   cb
                   :entity
                   nil
                   {}
                   nil)]

      (is (some? r-created-at))
      (is (inst? r-created-at))))

  (testing "allows override"
    (let [cb (sut/created-at-callback :created_at)]
      (is (= {:created_at :foo}
             (cb.proto/-before-save
              cb
              :entity
              {:created_at :foo}
              {:created_at :bar}
              nil))))
    (let [cb (sut/created-at-callback :created_at)]
      (is (= {:created_at :bar}
             (cb.proto/-before-save
              cb
              :entity
              {:created_at :foo}
              {:created_at :bar :set-created_at? true}
              nil))))
    (let [cb (sut/created-at-callback :created_at :update-created-at?)]
      (is (= {:created_at :bar}
             (cb.proto/-before-save
              cb
              :entity
              {:created_at :foo}
              {:created_at :bar :update-created-at? true}
              nil))))))
