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

(defn contains-subseq?
  "Returns the starting index of `ss` within `xs` or `nil` if `xs` does not
  contain `ss`

      (contains-subseq? [1 2 3 4 5 6] [1 2 3])
      => 0

      (contains-subseq? [1 2 3 4 5 6] [3 4 5])
      => 2

      (contains-subseq? [3 4 5] [5 4])
      => nil

  Useful if you need to determine if a list/vector has been prepended and/or
  appended to but retains all of its original elements in the same order."
  ([xs ss]
   (contains-subseq? 0 (vec xs) (vec ss)))
  ([i xs ss]
   (cond
     (> (count ss) (count xs)) nil
     (= ss (subvec xs 0 (count ss))) i
     :else (recur (inc i) (subvec xs 1) ss))))

(defn cassandra-list-collection-diff
  [empty x y]
  (let [subset-offset (contains-subseq? y x)
        ensure-type-match (if (vector? empty)
                            #(vec %)
                            #(if (list? %) % (apply list %)))]
    (t/map->CollectionColumnDiff
     (if subset-offset
       {:intersection (ensure-type-match x)
        :prepended (ensure-type-match (take subset-offset y))
        :appended (ensure-type-match (drop (+ subset-offset (count x)) y))
        :removed empty}
       {:intersection empty
        :prepended empty
        :appended (ensure-type-match y)
        :removed (ensure-type-match x)}))))

(defn cassandra-mapset-collection-diff
  [empty x y]
  (let [[removed added intersection] (diff x y)
        added-keyset (if (map? added)
                       (-> added keys set)
                       (or added empty))
        removed-keyset (if (map? removed)
                         (-> removed
                             keys
                             set
                             ;; need to omit 'only in x' keys
                             ;; for which we have a new value in y
                             (set/difference added-keyset))
                         (or removed #{}))]
    (t/map->CollectionColumnDiff
     {:intersection (into empty intersection)
      :prepended empty
      :appended (into empty added)
      :removed removed-keyset})))

(defn collection-diff
  [x y]
  (let [empty (or (empty x) (empty y))
        list-like? (sequential? empty)]
    (if list-like?
      (cassandra-list-collection-diff empty x y)
      (cassandra-mapset-collection-diff empty x y))))

(defn collection-changed?
  [{prpnd :prepended
    appnd :appended
    rmved :removed}]
  (or (seq prpnd)
      (seq appnd)
      (seq rmved)))

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

(defn diff-col-value
  "Given the old and new values for a column returns:

  * nil - if there is no change
  * [::change new-val] - if neither value is a collection.
  * [::change <CollectionColumnDiff>] - if either value is a collection.

  The <CollectionColumnDiff> record will consist of:

  * `:intersection` the values in both the old and new collection
  * `:prepended` the values added to the _beginning_ of the collection
  * `:appended` the values added to the _end_ of the collection
  * `:removed` the values removed

  In all cases the value of the field will be the same collection type as that
  of the underlying column.

  (Note that maps and sets have no concept of a ‘beginning’ or ‘end’ and as such
  any added values will always be treated as appends.)

  These <CollectionColumnDiff> values are used during statement preparation to
  construct the efficient—and tombstone avoiding—update statements."
  [old-v new-v]
  (if (or (coll? old-v) (coll? new-v))
    (let [ccd (collection-diff old-v new-v)]
      (when (collection-changed? ccd)
        [::changed ccd]))
    (when (not= old-v new-v)
      [::changed new-v])))

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
                                 [changed? dv] (diff-col-value ov v)]
                             (if changed?
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
           [coll-changed? coll-change] (when coll-col?
                                         (diff-col-value ov nv))]
       (cond
         tombstone-nil? rs
         coll-col? (if coll-changed?
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
