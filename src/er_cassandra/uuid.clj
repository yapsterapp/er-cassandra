(ns er-cassandra.uuid
  (:require
   [clj-time.coerce :as tc]
   [clj-uuid :as uuid])
  (:import
   [java.util UUID]
   [com.datastax.driver.core.utils UUIDs]
   [clojure.lang IPersistentVector IPersistentMap]))

(defn time->start-of-timeuuid
  [t]
  (-> t
      tc/to-long
      UUIDs/startOf))

(defn time->end-of-timeuuid
  [t]
  (-> t
      tc/to-long
      UUIDs/endOf))

(defn timeuuid-comparator
  "cassandra compares timeuuids by first comparing their
   timestamps and if they are equal comparing their
   binary encodings - this does the same"
  [timeuuid-a timeuuid-b]
  (let [ts-a (some-> timeuuid-a uuid/get-instant)
        ts-b (some-> timeuuid-b uuid/get-instant)
        tsc (compare ts-a ts-b)]
    (if (not= tsc 0)
      tsc
      (compare (str timeuuid-a) (str timeuuid-b)))))

(defn cassandra-uuid-compare
  "it seems cassandra always compares timeuuids as lower than
   non-timeuuids, so that's a thing"
  [a b]
  (let [a-timeuuid? (and (instance? UUID a)
                          (= 1 (clj-uuid/get-version a)))
        b-timeuuid? (and (instance? UUID b)
                          (= 1 (clj-uuid/get-version b)))]
    (cond
      (and a-timeuuid? b-timeuuid?)
      (timeuuid-comparator a b)

      :else
      (compare a b))))

(defprotocol ICassandraUUIDCompare
  (-compare [a b]))

(extend-protocol ICassandraUUIDCompare
  UUID
  (-compare [a b] (cassandra-uuid-compare a b))

  IPersistentVector
  (-compare [a b]
    (if (instance? IPersistentVector b)
      (cond
        (< (count a) (count b)) -1
        (> (count a) (count b)) 1
        :else
        (reduce (fn [r [a* b*]]
                  (let [cr (-compare a* b*)]
                    (if-not (zero? cr)
                      (reduced cr)
                      0)))
                0
                (map vector a b)))

      (throw (ex-info "vector compares only to vector"
                      {:a a :b b}))))

  Object
  (-compare [a b] (cassandra-uuid-compare a b))

  nil
  (-compare [a b] (compare a b)))
