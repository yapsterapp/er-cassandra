(ns er-cassandra.util.string
  (:require
   [clojure.string :as str])
  (:import
   [java.text Normalizer Normalizer$Form]))

(defn normalize-string
  [s]
  (when s
    (-> (str/trim s)
        (Normalizer/normalize Normalizer$Form/NFD)
        (str/replace #"\p{InCombiningDiacriticalMarks}+", "")
        .toLowerCase)))
