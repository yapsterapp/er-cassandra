(ns er-cassandra.key
  (:require
   [clojure.math.combinatorics :refer [cartesian-product]]
   [er-cassandra.util.vector :as v]))

(defn partition-key
  "given a primary key spec, return the partition key,
   which is the first element of the primary key spec"
  [key]
  (let [key (v/coerce key)]
    (v/coerce (first key))))

(defn cluster-key
  "given a primary key spec, return the cluster key"
  [key]
  (let [key (v/coerce key)
        ck (next key)]
    (when ck
      (vec ck))))

(defn flatten-key
  "coerce to a vector and remove any partition-key nesting"
  [key]
  (some-> key v/coerce flatten vec))

(defn extract-key-value
  "extract a key value from some combination of explicit value
   and a record"

  ([key record-or-key-value]
   (extract-key-value key record-or-key-value {}))

  ([key record-or-key-value {:keys [key-value]}]
   (let [key (flatten-key key)
         key-value (or (flatten-key key-value)
                       (if-not (map? record-or-key-value)
                         (flatten-key record-or-key-value)
                         (repeat (count key) nil)))
         record (when (map? record-or-key-value)
                  record-or-key-value)
         dkv (mapv (fn [k ev]
                     (if (some? ev) ev (get record k)))
                   key
                   key-value)]
     (when (and
            (= (count key) (count key-value))
            (not (some nil? dkv)))
       dkv))))

(defn remove-key-components
  "remove components from a [key key-value] pair, returning
   a new [key key-value] pair with the components removed"
  [key key-value remove-components]
  (let [kvs (map vector (flatten-key key) (flatten-key key-value))
        rcs (set (flatten-key remove-components))
        fkvs (filter (fn [[k v]] (not (contains? rcs k)))
                     kvs)]
    (when (> (count fkvs) 0)
      [(map first fkvs) (map second fkvs)])))

(defn key-equality-clause
  "return a Hayt when clause for key and key-value... if
   elements of key-value are lists then an :in clause will
   be used instead of :="
  [key key-value]
  (let [key (flatten-key key)
        key-value (v/coerce key-value)]
    (mapv (fn [k v]
            (if (sequential? v)
              [:in k v]
              [:= k v]))
          key
          key-value)))

(defn extract-key-equality-clause
  "returns a Hayt key equality clause for use in a (when...) form"

  ([key record-or-key-value]
   (extract-key-equality-clause key record-or-key-value {}))

  ([key record-or-key-value opts]
   (key-equality-clause
    key
    (extract-key-value key record-or-key-value opts))))

(defn extract-collection-key-components
  "col-colls - map of col to collection type :list/:set/:map/nil
   col - the col
   val-or-coll - the col value
   record - the record for reporting"
  [col-colls col val-or-coll record]
  (let [ctype (get col-colls col)]
    (case ctype
      nil [val-or-coll] ;; wrap for cartesian product

      ;; TODO this makes little sense - should it be dropped
      ;; or is it possible to make it make sense with [[key value]] ?
      :map ;; return the non-nil keys
      (if (or (nil? val-or-coll) (map? val-or-coll))
        (filter identity (keys val-or-coll))
        (throw (ex-info "col is not a map" {:col col
                                            :val-or-coll val-or-coll
                                            :record record})))

      :set ;; return all non-nil values
      (if (or (nil? val-or-coll) (set? val-or-coll))
        (disj val-or-coll nil)
        (throw (ex-info "col is not a set" {:col col
                                            :val-or-coll val-or-coll
                                            :record record})))

      :list ;; return all non-nil values
      (if (or (nil? val-or-coll) (sequential? val-or-coll))
        (filter identity val-or-coll)
        (throw (ex-info "col is not a list" {:col col
                                             :val-or-coll val-or-coll
                                             :record record})))

      (throw (ex-info "unknown collection type" {:col-colls col-colls
                                                 :col col
                                                 :val-or-coll val-or-coll
                                                 :record record})))))

(defn extract-key-value-collection
  "extracts a list of key-values. any column
   in the key may be a collection, and the final list
   will be the cartesian product of all values"
  ([key record col-colls]
   (when-let [kv (not-empty
                  (extract-key-value key record {}))]
     (let [key (flatten-key key)
           col-values (mapv (fn [k v]
                              (extract-collection-key-components
                               col-colls
                               k
                               v
                               record))
                            key
                            kv)]
       (apply cartesian-product col-values)))))

(defn has-key?
  "true if the record contains? keys for all the key components"
  [key record]
  (->> key
       flatten-key
       (map #(contains? record %))
       (every? identity)))
