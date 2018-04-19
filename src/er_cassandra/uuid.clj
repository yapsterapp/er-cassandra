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
  "Per [the source][src] Cassandra sorts UUIDs using the following comparisons,
  in order:

  1. version
  2. timestamp if both v1 UUIDs
  3. lexically, using an unsigned msb-to-lsb comparison

  [src]: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/UUIDType.java"
  [a b]
  (let [v-a (some-> a uuid/get-version)
        v-b (some-> b uuid/get-version)
        both-uuids? (and (some? v-a) (some? v-b))
        same-uuid-version? (and both-uuids? (= v-a v-b))]
    (cond
      (and same-uuid-version? (= 1 v-a))
      (timeuuid-comparator a b)

      same-uuid-version?
      (compare (str a) (str b))

      both-uuids?
      (compare v-a v-b)

      :else
      (throw (ex-info "huh?" {:a a :b b})))))

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
  (-compare [a b] (compare a b))

  nil
  (-compare [a b] (compare a b)))
