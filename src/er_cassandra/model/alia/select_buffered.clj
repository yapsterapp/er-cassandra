(ns er-cassandra.model.alia.select-buffered
  (:require
   [cats.context :refer [with-context]]
   [cats.core :refer [mlet return]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.key :as k]
   [er-cassandra.model.alia.select :as select]
   [er-cassandra.model.model-session :as ms]
   [er-cassandra.model.types :as t]
   [er-cassandra.record :as r]
   [er-cassandra.util.vector :as v]
   [prpr.stream :as stream])
  (:import
   [er_cassandra.model.model_session ModelSession]
   [er_cassandra.model.types Entity]))

(defn select-buffered-from-full-table
  "one buffered fetch - straight from a table. they key must be either
   a full primary key, or a partition key combined with some
   clustering key conditions (given as :where options)"
  ([^ModelSession session ^Entity entity table opts]
   (let [opts (-> opts
                  (assoc :row-generator (ms/->EntityInstanceRowGenerator)))]

     (r/select-buffered session (:name table) opts)))

  ([^ModelSession session ^Entity entity table key record-or-key-value opts]
   (let [opts (-> opts
                  (dissoc :key-value)
                  (assoc :row-generator (ms/->EntityInstanceRowGenerator)))]

     (r/select-buffered session (:name table) key record-or-key-value opts))))

(defn select-buffered-from-lookup-table
  "two fetches - use the lookup-key to get a stream of uber-keys, then
   map the records from the primary table with individual queries

   the lookup query may return multiple records, all of which will be
   dereferenced against the primary table, with nil results from
   dangling lookups being filtered out.

   this means that the lookup query may specify a partition-key and
   some clustering column condition (given as a :where option)"

  [^ModelSession session ^Entity entity table key record-or-key-value
   {:keys [buffer-size] :as opts}]
  (with-context deferred-context
    (mlet
      [:let [lkv (k/extract-key-value (or key (:key table))
                                      record-or-key-value
                                      opts)
             key (if lkv
                   (or key (:key table))
                   (k/partition-key (:key table)))
             key-value (if lkv
                         lkv
                         (k/extract-key-value
                          (k/partition-key (:key table))
                          record-or-key-value
                          opts))
             opts (dissoc opts :key-value)

             ;; query lookups with a downstream buffer to limit concurrency
             ;; of join queries
             lookup-opts (-> opts
                             (dissoc :columns)
                             (assoc :buffer-size (or buffer-size 25)))

             primary-opts (-> opts
                              (dissoc :where :only-if :order-by :limit)
                              (assoc :row-generator (ms/->EntityInstanceRowGenerator)))]

       lrs (r/select-buffered session
                              (:name table)
                              key
                              key-value
                              lookup-opts)]
      (->> lrs
           (stream/map (fn [lr]
                         (let [pkv (t/extract-uber-key-value entity lr)]
                           (r/select-one
                            session
                            (get-in entity [:primary-table :name])
                            (get-in entity [:primary-table :key])
                            pkv
                            primary-opts))))
           stream/realize-each
           (stream/filter identity)
           return))))

(defn select-buffered*
  "select records from primary or lookup tables as required

   returns a Deferred<Stream<record>>"
  ([^ModelSession session ^Entity entity] (select-buffered* session entity {}))
  ([^ModelSession session ^Entity entity opts]
   (select-buffered-from-full-table session
                                    entity
                                    (:primary-table entity)
                                    opts))
  ([^ModelSession session ^Entity entity key record-or-key-value {:keys [from] :as opts}]
   (let [key (v/coerce key)
         opts (dissoc opts :from)]
     (if from
       (if-let [full-table (or (t/is-primary-table entity from)
                               (t/is-secondary-table entity from))]
         (select-buffered-from-full-table session
                                          entity
                                          full-table
                                          key
                                          record-or-key-value
                                          opts)

         (if-let [lookup-table (or (t/is-unique-key-table entity from)
                                   (t/is-lookup-table entity from))]
           (select-buffered-from-lookup-table session
                                              entity
                                              lookup-table
                                              key
                                              record-or-key-value
                                              opts)

           (throw (ex-info
                   "no matching table"
                   {:reason [:fail
                             {:entity entity
                              :key key
                              :from from}
                             :no-matching-table]}))))

       (if-let [table (or (select/if-primary-key-table entity key)
                          (select/if-secondary-key-table entity key))]

         (select-buffered-from-full-table session
                                          entity
                                          table
                                          key
                                          record-or-key-value
                                          opts)

         (if-let [lookup-table (or (select/if-unique-key-table entity key)
                                   (select/if-lookup-table entity key))]

           (select-buffered-from-lookup-table session
                                              entity
                                              lookup-table
                                              key
                                              record-or-key-value
                                              opts)

           (throw (ex-info
                   "no matching key"
                   {:reason [:fail
                             {:entity entity
                              :key key}
                             :no-matching-key]}))))))))
