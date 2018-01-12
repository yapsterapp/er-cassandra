(ns er-cassandra.util.time
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as tc]
   [clj-time.format :as f]
   [clj-uuid :as uuid])
  (:import
   [java.util UUID]
   [com.datastax.driver.core.utils UUIDs]))

;; make things work with v1 UUIDs
(extend-protocol tc/ICoerce
  UUID
  (to-date-time [uuid]
    (-> uuid
        uuid/get-instant
        tc/from-date)))

(def timestamp-format (f/formatter "yyyy-MM-dd HH:mm:ssZ"))

(defn parse-timestamp
  [s]
  (f/parse timestamp-format s))

(defn unparse-timestamp
  [t]
  (->> t
       tc/to-date-time
       (f/unparse timestamp-format)))

;; a format similar to the clojure #inst format which captures
;; milliseconds and has no spaces
(def timestamp-format-utc-millis (f/formatter "yyyy-MM-dd'T'HH:mm:ss.SSS"))

(defn unparse-timestamp-utc-millis
  [t]
  (as-> t %
    (tc/to-date-time %)
    (t/to-time-zone % t/utc)
    (f/unparse timestamp-format-utc-millis %)))

(defn timeuuid-comparator
  "cassandra compares timeuuids by first comparing their
   timestamps and if they are equal comparing their
   binary encodings - this does the same"
  [timeuuid-a timeuuid-b]
  (let [ts-a (some-> timeuuid-a uuid/get-timestamp)
        ts-b (some-> timeuuid-b uuid/get-timestamp)
        tsc (compare ts-a ts-b)]
    (if (not= tsc 0)
      tsc
      (compare (str timeuuid-a) (str timeuuid-b)))))

(defn cassandra-uuid-compare
  "it seems cassandra always compares timeuuids as lower than
   non-timeuuids, so that's a thing"
  [a b]
  (let [ts-a (some-> a uuid/get-timestamp)
        ts-b (some-> b uuid/get-timestamp)]
    (cond
      (and ts-a ts-b)
      (timeuuid-comparator a b)

      (and (nil? ts-a) (nil? ts-b))
      (compare (str a) (str b))

      ts-a
      -1

      ts-b
      1

      :else (throw (ex-info "huh?" {:a a :b b})))))

(defn time->start-of-timeuuid
  [t]
  (-> t
      tc/to-long
      UUIDs/startOf))
