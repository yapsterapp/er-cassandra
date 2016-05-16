(ns er-cassandra.model.select
  (:require [manifold.deferred :as d]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [cats.labs.manifold :refer [deferred-context]]
            [qbits.alia :as alia]
            [qbits.alia.manifold :as aliam]
            [qbits.hayt :as h]
            [er-cassandra.key :as k]
            [er-cassandra.record :as r]
            [er-cassandra.model.types :as t]
            [er-cassandra.model.util :as util])
  (:import [er_cassandra.model.types Model]))

(defn if-primary-key-table
  [^Model model key]
  (when (or (t/satisfies-primary-key? (t/uber-key model) key)
            (t/satisfies-partition-key? (t/uber-key model) key))
    (:primary-table model)))

(defn if-secondary-key-table
  [^Model model key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key))
            t))
        (:secondary-tables model)))

(defn if-unique-key-table
  [^Model model key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key))
            t))
        (:unique-key-tables model)))

(defn if-lookup-key-table
  [^Model model key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key))
            t))
        (:lookup-key-tables model)))

(defn select-from-full-table
  "one fetch - straight from a table. they key must be either
   a full primary key, or a partition key combined with some
   clustering key conditions (given as :where options)"

  [session ^Model model table key record-or-key-value opts]
  (let [kv (k/extract-key-value key record-or-key-value opts)
        opts (dissoc opts :key-value)]
    (r/select session (:name table) key kv opts)))

(defn select-from-lookup-table
  "two fetches - use the lookup-key to get the uber-key, then
   get the record from the primary table.

   the lookup query may return multiple records, all of which will be
   dereferenced against the primary table, with nil results from
   dangling lookups being filtered out.

   this means that the lookup query may specify a partition-key and
   some clustering column condition (given as a :where option)"

  [session ^Model model table key record-or-key-value opts]
  (let [lkv (k/extract-key-value (or key (:key table)) record-or-key-value opts)
        key (if lkv (or key (:key table)) (k/partition-key (:key table)))
        key-value (if lkv
                    lkv
                    (k/extract-key-value
                     (k/partition-key (:key table)) record-or-key-value opts))
        opts (dissoc opts :key-value)
        lookup-opts (dissoc opts :columns)
        primary-opts (dissoc opts :where :only-if :order-by :limit)]
    (with-context deferred-context
      (mlet [lrs (r/select session
                             (:name table)
                             key
                             key-value
                             lookup-opts)
               pkvs (return
                     (map (fn [lr]
                            (t/extract-uber-key-value model lr))
                          lrs))
               prs (return
                    (map (fn [pkv]
                           (r/select-one
                            session
                            (get-in model [:primary-table :name])
                            (get-in model [:primary-table :key])
                            pkv
                            primary-opts))
                         pkvs))]
        (return
         (d/chain
          (util/combine-responses prs)
          (fn [rs] (filter identity rs))))))))

(defn select*
  "select records from primary or lookup tables as required"

  ([session ^Model model key record-or-key-value]
   (select* session model key record-or-key-value {}))

  ([session model key record-or-key-value {:keys [from] :as opts}]
   (let [key (k/make-sequential key)
         opts (dissoc opts :from)]
     (if from
       (if-let [full-table (or (t/is-primary-table model from)
                               (t/is-secondary-table model from))]
         (select-from-full-table session
                                 model
                                 full-table
                                 key
                                 record-or-key-value
                                 opts)

         (if-let [lookup-table (or (t/is-unique-key-table model from)
                                   (t/is-lookup-key-table model from))]
           (select-from-lookup-table session
                                     model
                                     lookup-table
                                     key
                                     record-or-key-value
                                     opts)

           (d/error-deferred (ex-info
                              "no matching table"
                              {:reason [:fail
                                        {:model model
                                         :key key
                                         :from from}
                                        :no-matching-table]}))))

       (if-let [table (or (if-primary-key-table model key)
                          (if-secondary-key-table model key))]

         (select-from-full-table session
                                 model
                                 table
                                 key
                                 record-or-key-value
                                 opts)

         (if-let [lookup-table (or (if-unique-key-table model key)
                                   (if-lookup-key-table model key))]

           (select-from-lookup-table session
                                     model
                                     lookup-table
                                     key
                                     record-or-key-value
                                     opts)

           (d/error-deferred (ex-info
                              "no matching key"
                              {:reason [:fail
                                        {:model model
                                         :key key}
                                        :no-matching-key]}))))))))

(defn select
  ([session ^Model model key record-or-key-value]
   (select session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   (t/run-deferred-callbacks
    model
    :after-load
    (select* session model key record-or-key-value opts)
    opts)))

(defn select-one
  "select a single record, using an index table if necessary"

  ([session ^Model model key record-or-key-value]
   (select-one session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   (with-context deferred-context
     (mlet [records (select session
                              model
                              key
                              record-or-key-value
                              (merge opts {:limit 1}))]
             (return (first records))))))

(defn select-many
  "issue one select-one query for each record-or-key-value and combine
   the responses"

  ([session ^Model model key record-or-key-values]
   (select-many session model key record-or-key-values {}))
  ([session ^Model model key record-or-key-values opts]
   (->> record-or-key-values
        (map (fn [record-or-key-value]
               (select-one session model key record-or-key-value)))
        util/combine-responses)))

(defn select-many-cat
  "issue one select query for each record-or-key-value and concatenates
   the responses"
  ([session ^Model model key record-or-key-values]
   (select-many-cat session model key record-or-key-values {}))
  ([session ^Model model key record-or-key-values opts]
   (->> record-or-key-values
        (map (fn [record-or-key-value]
               (select session model key record-or-key-value)))
        util/combine-seq-responses)))
