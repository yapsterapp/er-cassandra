(ns er-cassandra.model.callbacks.search-key-callback
  (:require
   [clojure.string :as str]
   [er-cassandra.util.string :refer [normalize-string]]))

(defn ^:private prepare-string
  [s]
  (let [s (normalize-string s)]
    (as-> s %
      (str/split % #"\s+")
      (conj % s)
      (filter not-empty %)
      (distinct %))))

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
  (let [contains-all-source-cols? (fn [r] (every? #(contains? r %) source-cols))]
    (fn [r]
      (if (contains-all-source-cols? r)
        (assoc r search-col
               (->> (map #(get r %) source-cols)
                    (mapcat extract-search-keys)
                    set))
        r))))
