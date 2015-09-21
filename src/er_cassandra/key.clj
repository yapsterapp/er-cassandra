(ns er-cassandra.key
  (:require
   [clojure.math.combinatorics :refer [cartesian-product]]))

(defn make-sequential
  [v]
  (cond (nil? v) v
        (sequential? v) v
        :else [v]))

(defn partition-key
  "given a primary key spec, return the partition key,
   which is the first element of the primary key spec"
  [key]
  (let [key (make-sequential key)]
    (make-sequential (first key))))

(defn extract-key-value*
  ([key record-or-key-value {:keys [key-value]}]
   (let [key (flatten (make-sequential key)) ;; flatten partition key
         key-value (or (make-sequential key-value)
                       (if-not (map? record-or-key-value)
                         (make-sequential record-or-key-value)
                         (repeat (count key) nil)))
         record (when (map? record-or-key-value)
                  record-or-key-value)
         dkv (map (fn [k ev]
                    (or ev (get record k)))
                  key
                  key-value)]
     (when (and
            (= (count key) (count key-value))
            (not (some nil? dkv)))
       dkv))))

(defn extract-key-value
  "extract a key value from some combination of explicit value
   and a record"

  ([key record-or-key-value]
   (extract-key-value key record-or-key-value {}))

  ([key record-or-key-value {:keys [collection] :as opts}]
   (extract-key-value* key record-or-key-value opts)))

(defn key-equality-clause
  [key key-value]
  (let [key (flatten (make-sequential key))
        key-value (make-sequential key-value)]
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
   (let [key (make-sequential key)
         kv (extract-key-value key record-or-key-value opts)]
     (key-equality-clause key kv))))

(defn extract-collection-key-components
  "col-colls - map of col to collection type :list/:set/:map/nil
   col - the col
   val-or-coll - the col value
   record - the record for reporting"
  [col-colls col val-or-coll record]
  (let [ctype (get col-colls col)]
    (case ctype
      nil [val-or-coll] ;; wrap for cartesian product

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
                  (extract-key-value* key record {}))]
     (let [key (flatten (make-sequential key))
           col-values (mapv (fn [k v]
                              (extract-collection-key-components
                               col-colls
                               k
                               v
                               record))
                            key
                            kv)]
       (apply cartesian-product col-values)))))
