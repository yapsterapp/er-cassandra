(ns er-cassandra.model.alia.select
  (:require [manifold.deferred :as d]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [cats.labs.manifold :refer [deferred-context]]
            [er-cassandra.session :as s]
            [er-cassandra.key :as k]
            [er-cassandra.record :as r]
            [er-cassandra.model.types :as t]
            [er-cassandra.model.model-session :as ms]
            [er-cassandra.model.util :as util])
  (:import
   [er_cassandra.session Session]
   [er_cassandra.model.types Entity]))

(defn if-primary-key-table
  [^Entity entity key]
  (when (or (t/satisfies-primary-key? (t/uber-key entity) key)
            (t/satisfies-partition-key? (t/uber-key entity) key)
            (t/satisfies-cluster-key? (t/uber-key entity) key))
    (:primary-table entity)))

(defn if-secondary-key-table
  [^Entity entity key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key)
                    (t/satisfies-cluster-key? (:key t) key))
            t))
        (:secondary-tables entity)))

(defn if-unique-key-table
  [^Entity entity key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key)
                    (t/satisfies-cluster-key? (:key t) key))
            t))
        (:unique-key-tables entity)))

(defn if-lookup-key-table
  [^Entity entity key]
  (some (fn [t]
          (when (or (t/satisfies-primary-key? (:key t) key)
                    (t/satisfies-partition-key? (:key t) key)
                    (t/satisfies-cluster-key? (:key t) key))
            t))
        (:lookup-key-tables entity)))

(def select-err-msg
  "Versioned tables can only perform selects when :limit option is set to 1.")

(defn select-from-full-table
  "one fetch - straight from a table. they key must be either
   a full primary key, or a partition key combined with some
   clustering key conditions (given as :where options)"

  [^Session session ^Entity entity table key record-or-key-value opts]
  (let [kv (k/extract-key-value key record-or-key-value opts)
        opts (-> opts
                 (dissoc :key-value)
                 (assoc :row-generator (ms/->EntityInstanceRowGenerator)))]
    (when (and (:versioned? entity) (not= 1 (:limit opts)))
      (throw (ex-info select-err-msg {:model entity :opts opts})))
    (r/select session (:name table) key kv opts)))

(defn select-from-lookup-table
  "two fetches - use the lookup-key to get the uber-key, then
   get the record from the primary table.

   the lookup query may return multiple records, all of which will be
   dereferenced against the primary table, with nil results from
   dangling lookups being filtered out.

   this means that the lookup query may specify a partition-key and
   some clustering column condition (given as a :where option)"

  [^Session session ^Entity entity table key record-or-key-value opts]
  (let [lkv (k/extract-key-value (or key (:key table)) record-or-key-value opts)
        key (if lkv (or key (:key table)) (k/partition-key (:key table)))
        key-value (if lkv
                    lkv
                    (k/extract-key-value
                     (k/partition-key (:key table)) record-or-key-value opts))
        opts (dissoc opts :key-value)
        lookup-opts (dissoc opts :columns)
        primary-opts (-> opts
                         (dissoc :where :only-if :order-by :limit)
                         (assoc :row-generator (ms/->EntityInstanceRowGenerator)))]
    (with-context deferred-context
      (mlet [lrs (r/select session
                             (:name table)
                             key
                             key-value
                             lookup-opts)
               pkvs (return
                     (map (fn [lr]
                            (t/extract-uber-key-value entity lr))
                          lrs))
               prs (return
                    (map (fn [pkv]
                           (r/select-one
                            session
                            (get-in entity [:primary-table :name])
                            (get-in entity [:primary-table :key])
                            pkv
                            primary-opts))
                         pkvs))]
        (return
         (d/chain
          (util/combine-responses prs)
          (fn [rs] (filter identity rs))))))))

(defn select*
  "select records from primary or lookup tables as required"
  [^Session session ^Entity entity key record-or-key-value {:keys [from] :as opts}]
   (let [key (k/make-sequential key)
         opts (dissoc opts :from)]
     (if from
       (if-let [full-table (or (t/is-primary-table entity from)
                               (t/is-secondary-table entity from))]
         (select-from-full-table session
                                 entity
                                 full-table
                                 key
                                 record-or-key-value
                                 opts)

         (if-let [lookup-table (or (t/is-unique-key-table entity from)
                                   (t/is-lookup-key-table entity from))]
           (select-from-lookup-table session
                                     entity
                                     lookup-table
                                     key
                                     record-or-key-value
                                     opts)

           (d/error-deferred (ex-info
                              "no matching table"
                              {:reason [:fail
                                        {:entity entity
                                         :key key
                                         :from from}
                                        :no-matching-table]}))))

       (if-let [table (or (if-primary-key-table entity key)
                          (if-secondary-key-table entity key))]

         (select-from-full-table session
                                 entity
                                 table
                                 key
                                 record-or-key-value
                                 opts)

         (if-let [lookup-table (or (if-unique-key-table entity key)
                                   (if-lookup-key-table entity key))]

           (select-from-lookup-table session
                                     entity
                                     lookup-table
                                     key
                                     record-or-key-value
                                     opts)

           (d/error-deferred (ex-info
                              "no matching key"
                              {:reason [:fail
                                        {:entity entity
                                         :key key}
                                        :no-matching-key]})))))))
