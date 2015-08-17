(ns er-cassandra.model.select
  (:require [manifold.deferred :as d]
            [cats.core :as m]
            [cats.monad.either :as either]
            [cats.monad.deferred :as dm]
            [qbits.alia :as alia]
            [qbits.alia.manifold :as aliam]
            [qbits.hayt :as h]
            [er-cassandra.key :as k]
            [er-cassandra.record-either :as r]
            [er-cassandra.model.types :as t])
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

(defn if-unique-key-table
  [^Model model key]
  (some (fn [t]
          (when (= (:key t) key)
            t))
        (:unique-key-tables model)))

(defn if-lookup-key-table
  [^Model model key]
  (some (fn [t]
          (when (= (:key t) key)
            t))
        (:lookup-key-tables model)))

(defn select-from-full-table
  "one fetch - straight from a table"

  [session ^Model model table record-or-key-value opts]
  (let [kv (k/extract-key-value (:key table) record-or-key-value opts)]
    (r/select session (:name table) (:key table) kv (dissoc opts :key-value))))

(defn select-from-lookup-table
  "two fetches - use the lookup-key to get the uber-key, then
   get the record from the primary table"

  [session ^Model model table record-or-key-value opts]
  (let [lkv (k/extract-key-value (:key table) record-or-key-value opts)]
    (m/with-monad dm/either-deferred-monad
      (m/mlet [lr (r/select-one session (:name table) (:key table) lkv)
               pkv (m/return
                    (when lr
                      (t/extract-uber-key-value model lr)))]
              (if pkv
                (r/select session
                          (get-in model [:primary-table :name])
                          (get-in model [:primary-table :key])
                          pkv
                          (dissoc opts :key-value))
                (m/return nil))))))

(defn select*
  "select records from primary or lookup tables as required"

  ([session ^Model model key record-or-key-value]
   (select* session model key record-or-key-value {}))

  ([session model key record-or-key-value opts]
   (let [key (k/make-sequential key)]
     (if-let [table (or (if-primary-key-table model key)
                        (if-secondary-key-table model key))]

       (select-from-full-table session
                               model
                               table
                               record-or-key-value
                               opts)

       (if-let [lookup-table (or (if-unique-key-table model key)
                                 (if-lookup-key-table model key))]

         (select-from-lookup-table session
                                   model
                                   lookup-table
                                   record-or-key-value
                                   opts)

         (dm/with-value (either/left [:fail
                                      {:model model
                                       :key key}
                                      :no-matching-key])))))))

(defn select
  ([session ^Model model key record-or-key-value]
   (select session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   (t/run-callbacks
    model
    :after-load
    (select* session model key record-or-key-value opts))))

(defn select-one
  "select a single record, using an index table if necessary"

  ([session ^Model model key record-or-key-value]
   (select-one session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   (m/with-monad dm/either-deferred-monad
     (m/mlet [records (select session model key record-or-key-value opts)]
             (m/return (first records))))))
