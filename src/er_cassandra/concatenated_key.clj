(ns er-cassandra.concatenated-key
  (:require
   [clojure.string :as str]
   [er-cassandra.util.time :as cut]))

;; fns for concatenating key components into a correctly sorting Text
;; key to use as the additional component of the PK of an MV

(defprotocol IConcatenatedKeySegment
  (-str-rep [v]))

(extend-type String
  IConcatenatedKeySegment
  (-str-rep [v] v))

(extend-type Boolean
  IConcatenatedKeySegment
  (-str-rep [v] (if v "true" "false")))

(extend-type java.util.Date
  IConcatenatedKeySegment
  (-str-rep [v]
    (cut/unparse-timestamp-utc-millis v)))

(extend-type java.lang.Object
  IConcatenatedKeySegment
  (-str-rep [v]
    (str v)))

(def default-separator "/")

(defn concatenate-keys
  "concatenate some key values with SQL NULL semantics - any nil
   key-col-val results in a nil result"
  ([key-col-vals]
   (concatenate-keys default-separator key-col-vals))

  ([separator key-col-vals]
   (when (every? some? key-col-vals)
     (->> key-col-vals
          (map -str-rep)
          (str/join separator)))))
