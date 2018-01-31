(ns er-cassandra.util.time
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as tc]
   [clj-time.format :as f]
   [clj-uuid :as uuid]
   [er-cassandra.uuid :as c.uuid])
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

(def time->start-of-timeuuid c.uuid/time->start-of-timeuuid)

(def timeuuid-comparator c.uuid/timeuuid-comparator)

(def cassandra-uuid-compare c.uuid/cassandra-uuid-compare)
