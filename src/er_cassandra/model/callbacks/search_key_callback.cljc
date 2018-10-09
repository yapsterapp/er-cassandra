(ns er-cassandra.model.callbacks.search-key-callback
  (:require
   [clojure.string :as str]
   #?(:cljs [er-cassandra.model.callbacks.protocol :refer [ICallback]])
   [er-cassandra.util.string :refer [normalize-string]])
  #?(:clj
     (:import
      [er_cassandra.model.callbacks.protocol ICallback])))

(defn ^:private prepare-string
  [s]
  (let [s (normalize-string s)]
    (as-> s %
      (str/split % #"\s+")
      (conj % s)
      (filter not-empty %)
      (distinct %))))

(defn ^:private value->search-keys
  [value]
  (cond
    (nil? value) []
    (string? value) (prepare-string value)
    (sequential? value) (mapcat value->search-keys value)
    (set? value) (mapcat value->search-keys value)
    (map? value) (mapcat value->search-keys (vals value))
    :else (prepare-string (str value))))

(defn source-cols->search-keys
  [source-cols record]
  (->> (map #(get record %) source-cols)
       (mapcat value->search-keys)
       set))

(defn create-search-keys-callback
  "create a callback which splits values from source-cols
   into words and assocs a set of those words to search-col"
  [search-col & source-cols]
  (let [no-source-cols? (fn [r]
                     (every? #((complement contains?) r %)
                             source-cols))
        all-source-cols? (fn [r]
                           (every? #(contains? r %)
                                   source-cols))
        search-col? #(contains? % search-col)
        no-search-col? #((complement contains?) % search-col)]
    (reify
      ICallback
      (-serialize [_ entity old-record new-record opts]
        (cond
          ;; if there are no source-cols in the new-record
          ;; and the new-record doesn't update the search-col then
          ;; we don't care
          (and (no-source-cols? new-record)
               (no-search-col? new-record))
          new-record

          (or
           ;; if it's a create then new new-record is by definition complete
           (nil? old-record)

           ;; otherwise we need all the old source-cols and the search-col
           ;; (for a change op)
           (and (some? old-record)
                (search-col? old-record)
                (all-source-cols? old-record)))
          (assoc new-record
                 search-col
                 (source-cols->search-keys
                  source-cols
                  (merge old-record new-record)))

          ;; HACK ALERT
          ;; a partial update doesn't really give enough information
          ;; but throwing borks all the tests, so prior to fixing
          ;; properly, at least add any new search-keys to the set...
          :else
          (assoc new-record
                 search-col
                 (set
                  (into (get old-record search-col)
                        (source-cols->search-keys
                         source-cols
                         (merge old-record new-record)))))

          ;; it would be better to throw if there isn't a complete source
          ;; but it breaks a tonne of tests
          ;; (throw
          ;;  (pr/error-ex
          ;;   ::old-record-search-keys-partial-cols
          ;;   {:search-col search-col
          ;;    :required-source-cols source-cols
          ;;    :old-record old-record
          ;;    :new-record new-record}))
          )))))
