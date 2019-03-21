(ns er-cassandra.model.alia.minimal-change
  (:require
   [clojure.data :refer [diff]]
   [clojure.set :as set]
   [prpr.promise :as pr]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.lookup :as lookup]
   [taoensso.timbre :refer [info warn]])
  (:import
   [er_cassandra.model.types Entity]))

(defn non-positional-diff
  "Returns a [[diff]] of the unique values in the given collections. If the
  given collections don't have unique value constraints—e.g. aren’t maps or
  sets—they will be coerced to sets before being diffed with the results
  returned as a matching collection type to the input.

  This gives the `fn` it’s non-positional characteristics: collections are
  compared based on the contents but not the order of those contents even if the
  collections have ordering semantics.

  Returns a [[diff]] three element tuple of:

      [<key/value pairs or values only in `x`>
      [<key/value pairs or values only in `y`>
       <key/value pairs or values common to both>]

  Given two maps this `fn` behaves as per a normal [[diff]]:

      (non-positional-diff
       {:a 1
        :b 2
        :c 3}
       {:a 2
        :b 2
        :d 4})
      => [;; only in `x`
          {:a 1
           :c 3}
          ;; only in `y`
          {:a 2
           :d 4}
          ;; common
          {:b 2}]

  Given two `seq`s that can be coerced to Sets the behavior differs:

      (non-positional-diff
       [1 2 3 4]
       '(2 3 1 5))
      => [;; only in `x`
          [4]
          ;; only in `y`
          [5]
          ;; common
          [1 2 3]]

  Compared to a traditional [[diff]] which would yield:

      (diff
       [1 2 3 4]
       '(2 3 1 5))
      => [;; no values are in common positions so the entire collection is ‘different’
          [1 2 3 4]
          [2 3 1 5]
          nil]

  Note that in the above example the returned tuple contains vectors as the
  first argument, `x`, is a vector."
  [x y]
  (let [coll-x (if (map? x) x (set x))
        coll-y (if (map? y) y (set y))
        empty (or (empty x) (empty y))
        results (diff coll-x coll-y)]
    (map
     (fn [xs]
       (if (or (map? x) (set? x))
         xs
         (into empty xs)))
     results)))

(defn change-cols
  "returns non-key columns which have changes between
   old-record and record"
  [key-col-set
   old-record
   record]
  (let [record-col-set (->> record keys set)
        other-col-set (set/difference
                       record-col-set
                       key-col-set)]

    (->> other-col-set
         (filter
          #(not= (get old-record %) (get record %))))))

(defn as-coll-without-nils
  [xs]
  (if (or (nil? xs) (map? xs))
    xs
    (into
     (empty xs)
     (filter identity xs))))

(defn diff-col-value
  "Given the old and new values for a column returns either:

  * The new value, if neither value is a collection.
  * A [[CollectionColumnDiff]] record if either value is a collection.

  The [[CollectionColumnDiff]] record will consist of:

  * `:intersection` the values in both the old and new collection
  * `+` the values added
  * `-` the values removed

  In all cases the value of the field will be the same collection type as that
  of the underlying column.

  Note that the later two keys above are the Clojure core `fn`s `+` and `-`,
  these are used as keys as Hayt recognizes them and can translate them to the
  equivalent CQL.

  These [[CollectionColumnDiff]] values are used during statement preparation to
  efficiently construct CQL `SET column = column [+-] values` statement
  fragments which, as they alter the existing column value rather than replace
  it, do not result in the generation of tombstone entries."
  [old-v new-v]
  (if (or (coll? old-v) (coll? new-v))
    (let [[removed added intsx] (non-positional-diff old-v new-v)
          intsx-set (or (as-coll-without-nils intsx) (empty (or old-v new-v)))
          added-set (as-coll-without-nils added)
          removed-set (if (map? removed)
                        (set/difference
                         (set (keys removed))
                         (set (keys added)))
                        (as-coll-without-nils removed))
          changed? (or (seq added-set) (seq removed-set))]
      (when changed?
        (t/map->CollectionColumnDiff
         (merge
          {:intersection intsx-set}
          (when (seq added-set)
            {+ added-set})
          (when (seq removed-set)
            {- removed-set})))))
    new-v))

(defn minimal-change-for-table
  "return a minimal change record for a table - removing columns
   that do not need to be written. if nil is returned then nothing
   needs to be written.
   - if there are changes it contains the key cols and changed cols
   - if there are no changes and there was an old record it is nil
   - if there are no changes and there was no old record it contains
     just the key cols"
  [{t-k :key
    :as table}
   old-record
   record]
  (let [key-col-set (->> t-k flatten set)
        ch-cols (change-cols key-col-set old-record record)]
    (when (or (empty? old-record)
              (not-empty ch-cols))
      (let [key-cols (select-keys record key-col-set)
            change-cols (reduce
                         (fn [rs [k v]]
                           (let [ov (get old-record k)
                                 dv (diff-col-value ov v)
                                 niled? (and (nil? v) (some? ov))
                                 coll-col? (or (coll? ov) (coll? v))
                                 val-change? (or niled? (some? dv))
                                 coll-change? (and coll-col? (seq dv))
                                 real-change? (if coll-col? coll-change? val-change?)]
                             (if real-change?
                               (assoc rs k dv)
                               rs)))
                         {}
                         (select-keys record ch-cols))]
        (merge key-cols change-cols)))))

(defn avoid-tombstone-change-for-table
  [{t-k :key
    :as table}
   old-record
   record]
  (reduce
   (fn [rs [k nv]]
     (let [ov (get old-record k)
           coll-col? (or (coll? ov) (coll? nv))
           tombstone-nil? (and (nil? ov) (nil? nv))
           coll-change (when coll-col?
                         (diff-col-value ov nv))]
       (cond
         tombstone-nil? rs
         coll-col? (if (seq coll-change)
                     (assoc rs k coll-change)
                     rs)
         (or (some? ov) (some? nv)) (assoc rs k nv)
         :else rs)))
   {}
   record))


;; TODO
;; change the secondary and lookup maintenance to not use
;; minimal-change-for-table... instead to always write
;; and use avoid-tombstone-change-for-table
