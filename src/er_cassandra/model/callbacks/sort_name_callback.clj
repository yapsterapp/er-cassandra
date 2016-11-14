(ns er-cassandra.model.callbacks.sort-name-callback
  (:require
   [clojure.string :as str])
  (:import
   [java.text Normalizer Normalizer$Form]))

(defn normalize-string
  [s]
  (-> s
      str/trim
      (Normalizer/normalize Normalizer$Form/NFD)
      (str/replace #"\p{InCombiningDiacriticalMarks}+", "")
      .toLowerCase))

(defn create-sort-name-callback
  [sort-col name-col]
  (fn [r]
    (assoc r sort-col (normalize-string (get r name-col)))))
