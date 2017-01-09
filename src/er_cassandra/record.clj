(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [trace debug info warn error]]
   [schema.core :as s]
   [clojure.set :as set]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :refer [flatten-key extract-key-equality-clause]]
   [er-cassandra.session :as session])
  (:import
   [er_cassandra.session Session]
   [qbits.hayt.cql CQLRaw]))

;; low-level record-based cassandra statement generation and execution
;;
;; keys can be extracted from records, provided explicitly or mixed.
;;
;; execution is async and returns a manifold Deferred

(defn combine-where
  [& clauses]
  (into []
        (->> clauses
             (filter identity)
             (apply concat))))

(s/defschema WhereClauseSchema
  [(s/one (s/enum :<= :< := :>= :> :in) :where-op)
   (s/one s/Keyword :where-col)
   (s/one s/Any :where-val)])

(s/defschema WhereSchema
  [WhereClauseSchema])

(s/defschema UpdateColumnSchema
  s/Keyword)

(s/defschema UpdateColumnsSchema
  [(s/one UpdateColumnSchema :first-col) UpdateColumnSchema])

(s/defschema SelectColumnSchema
  (s/conditional
   keyword? s/Keyword
   :else CQLRaw))

(s/defschema SelectColumnsSchema
  [(s/one SelectColumnSchema :first-col) SelectColumnSchema])

(s/defschema OrderByClauseSchema
  [(s/one s/Keyword :order-by-col)
   (s/one (s/enum :asc :desc) :order-by-dir)])

(s/defschema OrderBySchema
  [OrderByClauseSchema])

(s/defschema LimitSchema s/Int)

(s/defschema FullTableSelectOptsSchema
  {(s/optional-key :columns) SelectColumnsSchema
   (s/optional-key :limit) LimitSchema})

(s/defschema SelectOptsSchema
  (merge
   FullTableSelectOptsSchema
   {(s/optional-key :where) WhereSchema
    (s/optional-key :order-by) OrderBySchema}))

(s/defschema PartitionKeySchema
  (s/conditional
   keyword?
   s/Keyword

   :else
   [(s/one s/Keyword :partition-key-first-component)
    s/Keyword]))

(s/defschema KeySchema
  (s/conditional
   keyword?
   s/Keyword

   :else
   [(s/one PartitionKeySchema :partition-key)
    s/Keyword]))

(s/defschema KeyValueComponentSchema
  (s/pred some? :key-value-component))

(s/defschema RecordSchema
  {s/Keyword s/Any})

(s/defschema RecordOrKeyValueSchema
  (s/conditional
   map?
   RecordSchema

   sequential?
   [(s/one KeyValueComponentSchema :first-key-value-component)
    KeyValueComponentSchema]

   :else
   KeyValueComponentSchema))

(s/defn select-statement
  "returns a Hayt select statement"

  ([table :- s/Keyword]
   (select-statement table {}))

  ([table :- s/Keyword
    {:keys [limit columns] :as opts} :- FullTableSelectOptsSchema]

   (h/select table
             (when columns (apply h/columns columns))
             (when limit (h/limit limit))))

  ([table :- s/Keyword
    key :- KeySchema
    record-or-key-value :- RecordOrKeyValueSchema]
   (select-statement table key record-or-key-value {}))

  ([table :- s/Keyword
    key :- KeySchema
    record-or-key-value :- RecordOrKeyValueSchema
    {:keys [where columns order-by limit] :as opts} :- SelectOptsSchema]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
         where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-clause (combine-where key-clause where-clause)]
     (h/select table
               (h/where where-clause)
               (when columns (apply h/columns columns))
               (when order-by (apply h/order-by order-by))
               (when limit (h/limit limit))))))

(defn select
  "select records"

  ([^Session session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    opts]
   (session/execute
    session
    (select-statement table
                      key
                      record-or-key-value
                      (select-keys opts [:columns :where :only-if :order-by :limit]))
    (dissoc opts :columns :where :only-if :order-by :limit))))

;; TODO change to return a Deferred<Stream> which turns out to be a lot more
;; convenient to work with

(defn select-buffered
  "select a stream of records

   if :buffer-size opt is given, a *downstream* buffer will be applied to
   the query stream. the query-buffer will be sized by the :fetch-size opt
   if given"

  ([^Session session table] (select-buffered session table {}))

  ([^Session session table opts]
   (let [stmt (select-statement
               table
               (select-keys opts [:columns :limit]))]
     (session/execute-buffered
      session
      stmt
      (-> opts
          (dissoc :columns :limit)))))

  ([^Session session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {:keys [buffer-size] :as opts}]
   (let [stmt (select-statement
               table
               key
               record-or-key-value
               (select-keys opts [:columns :where :only-if :order-by :limit]))]
     (session/execute-buffered
      session
      stmt
      (-> opts
          (dissoc :columns :where :only-if :order-by :limit))))))

(defn select-one
  "select a single record"

  ([^Session session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))

  ([^Session session table key record-or-key-value opts]
   (d/chain
    (select session table key record-or-key-value (merge opts {:limit 1}))
    first)))

(s/defschema UpsertUsingSchema
  {(s/optional-key :ttl) s/Int
   (s/optional-key :timestamp) s/Int})

(s/defschema InsertOptsSchema
  {(s/optional-key :if-not-exists) s/Bool
   (s/optional-key :using) UpsertUsingSchema})

(s/defn insert-statement
  "returns a Hayt insert statement"

  ([table :- s/Keyword
    record :- RecordSchema] (insert-statement table record {}))

  ([table :- s/Keyword
    record :- RecordSchema
    {:keys [if-not-exists using] :as opts} :- InsertOptsSchema]
   (h/insert table
             (h/values record)
             (when if-not-exists (h/if-not-exists true))
             (when (not-empty using) (apply h/using (flatten (seq using)))))))

(defn insert
  "insert a single record"

  ([^Session session table record]
   (insert session table record {}))

  ([^Session session table record opts]
   (d/chain
    (session/execute
     session
     (insert-statement
      table
      record
      (select-keys opts [:if-not-exists :using]))
     (dissoc opts :if-not-exists :using))
    first)))

(s/defschema UpdateOptsSchema
  {(s/optional-key :only-if) WhereSchema
   (s/optional-key :if-exists) s/Bool
   (s/optional-key :if-not-exists) s/Bool
   (s/optional-key :using) UpsertUsingSchema
   (s/optional-key :set-columns) UpdateColumnsSchema})

(s/defn update-statement
  "returns a Hayt update statement"

  ([table :- s/Keyword
    key :- KeySchema
    record :- RecordSchema] (update-statement table key record {}))

  ([table :- s/Keyword
    key :- KeySchema
    record :- RecordSchema
    {:keys [only-if
            if-exists
            if-not-exists
            using
            set-columns] :as opts} :- UpdateOptsSchema]
   (let [key-clause (extract-key-equality-clause key record opts)
         set-cols (if (not-empty set-columns)
                    (select-keys record set-columns)
                    (apply dissoc record (flatten-key key)))

         stmt (h/update table
                        (h/set-columns set-cols)
                        (h/where key-clause)
                        (when only-if (h/only-if only-if))
                        (when if-exists (h/if-exists true))
                        (when if-not-exists (h/if-exists false))
                        (when (not-empty using) (apply h/using (flatten (seq using)))))]
     stmt)))

(defn update
  "update a single record"

  ([^Session session table key record]
   (update session table key record {}))

  ([^Session session table key record opts]
   (d/chain
    (session/execute
     session
     (update-statement
      table
      key
      record
      (select-keys opts [:only-if :if-exists :using :set-columns]))
     (dissoc opts :only-if :if-exists :using :set-columns))
    first)))

(s/defschema DeleteUsingSchema
  {(s/optional-key :timestamp) s/Int})

(s/defschema DeleteOptsSchema
  {(s/optional-key :only-if) WhereSchema
   (s/optional-key :if-exists) s/Bool
   (s/optional-key :using) DeleteUsingSchema
   (s/optional-key :where) WhereSchema})

(s/defn delete-statement
  "returns a Hayt delete statement"

  ([table :- s/Keyword
    key :- KeySchema
    record-or-key-value :- RecordOrKeyValueSchema]
   (delete-statement table key record-or-key-value {}))

  ([table :- s/Keyword
    key :- KeySchema
    record-or-key-value :- RecordOrKeyValueSchema
    {:keys [only-if if-exists using where] :as opts} :- DeleteOptsSchema]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)]
     (h/delete table
               (h/where (combine-where key-clause where))
               (when only-if (h/only-if only-if))
               (when if-exists (h/if-exists true))
               (when (not-empty using) (apply h/using (flatten (seq using))))))))

(defn delete
  "delete a record"

  ([^Session session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([^Session session table key record-or-key-value opts]
   (d/chain
    (session/execute
     session
     (delete-statement
      table
      key
      record-or-key-value
      (select-keys opts [:only-if :if-exists :using :where]))
     (dissoc opts :only-if :if-exists :using :where))
    first)))
