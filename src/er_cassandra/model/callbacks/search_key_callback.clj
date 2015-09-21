(ns er-cassandra.model.callbacks.search-key-callback
  (:require
   [clojure.string :as str])
  (:import
   [java.text Normalizer Normalizer$Form]))

(defn normalize-string
  [s]
  (-> s
      (Normalizer/normalize Normalizer$Form/NFD)
      (str/replace #"\p{InCombiningDiacriticalMarks}+", "")
      .toLowerCase))

(defn ^:private prepare-string
  [s]
  (-> s
      normalize-string
      (str/split #"\s+")))

(defn ^:private extract-search-keys
  [value]
  (cond
    (nil? value) []
    (string? value) (prepare-string value)
    (sequential? value) (mapcat extract-search-keys value)
    (set? value) (mapcat extract-search-keys value)
    (map? value) (mapcat extract-search-keys (vals value))
    :else (prepare-string (str value))))

(defn create-search-keys-callback
  "create a callback which splits values from source-cols
   into words and assocs a set of those words to search-col"
  [search-col & source-cols]
  (fn [r]
    (let [search-keys (->> source-cols
                           (map #(get r %))
                           (mapcat extract-search-keys)
                           set)]
      (assoc r search-col search-keys))))
