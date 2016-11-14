(ns er-cassandra.time
  (:require
   [clj-time.format :as f]))

(def timestamp-format (f/formatter "yyyy-MM-dd HH:mm:ssZ"))

(defn parse-timestamp
  [s]
  (f/parse timestamp-format s))

(defn unparse-timestamp
  [t]
  (f/unparse timestamp-format t))
