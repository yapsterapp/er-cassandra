(ns er-cassandra.model.select
  (:require [manifold.deferred :as d]
            [qbits.alia :as alia]
            [qbits.alia.manifold :as aliam]
            [qbits.hayt :as h]
            [er-cassandra.key :as k]
            [er-cassandra.record :as r]
            [er-cassandra.model.types])
  (:import [er_cassandra.model.types Model]))

(defn if-primary-key-table
  [^Model model key]
  (when (= (get-in model [:primary-table :key])
           key)
    (:primary-table model)))

(defn if-secondary-key-table
  [^Model model key]
  (some (fn [t]
          (when (= (:key t) key)
            t))
        (:secondary-tables model)))

(defn if-lookup-key-table
  [^Model model key]
  (some (fn [t]
          (when (= (:key t) key)
            t))
        (:lookup-tables model)))

(defn select-from-full-table
  "one fetch - straight from a table"

  [session ^Model model table record-or-key-value opts]
  (let [kv (k/extract-key-value (:key table) record-or-key-value opts)]
    (r/select session (:name table) (:key table) kv (dissoc opts :key-value))))

(defn select-from-lookup-table
  "two fetches - use the lookup-key to get the uber-key, then
   get the record from the primary table"

  [session ^Model model table record-or-key-value opts]
  (let [lkv (k/extract-key-value (:key table) record-or-key-value opts)
        lr-def (r/select-one session (:name table) (:key table) lkv)]
    (d/chain lr-def
             (fn [lr]
               (when lr ;; ignore dangling lookups
                 (when-let [pkv (k/extract-key-value
                                 (get-in model [:primary-table :key])
                                 lr)]
                   (r/select session
                             (get-in model [:primary-table :name])
                             (get-in model [:primary-table :key])
                             pkv
                             (dissoc opts :key-value))))))))

(defn select
  "select records from primary or lookup tables as required"

  ([session ^Model model key record-or-key-value]
   (select session model key record-or-key-value {}))

  ([session model key record-or-key-value opts]
   (let [key (k/make-sequential key)]
     (if-let [table (or (if-primary-key-table model key)
                        (if-secondary-key-table model key))]

       (select-from-full-table session
                               model
                               table
                               record-or-key-value
                               opts)

       (if-let [lookup-table (if-lookup-key-table model key)]

         (select-from-lookup-table session
                                   model
                                   lookup-table
                                   record-or-key-value
                                   opts)

         (let [r (d/deferred)]
           (d/error! r
                     (ex-info "no model table matches key"
                              {:model model :key key}))
           r))))))

(defn select-one
  "select a single record, using an index table if necessary"

  ([session ^Model model key record-or-key-value]
   (select-one session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   (d/chain (select session model key record-or-key-value opts)
            first)))
