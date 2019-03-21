(ns er-cassandra.record.statement
  (:require
   [plumbing.core :refer [assoc-when]]
   [taoensso.timbre :refer [trace debug info warn error]]
   [schema.core :as s]
   [clojure.set :as set]
   [manifold.deferred :as d]
   [manifold.stream :as stream]
   [qbits.hayt :as h]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :refer [flatten-key extract-key-equality-clause]]
   [er-cassandra.model.types :as t]
   [er-cassandra.session :as session]
   [prpr.promise :as pr :refer [ddo]]
   [cats.context :refer [with-context]]
   [cats.core :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.record.schema :as sch])
  (:import
   [qbits.hayt.cql CQLRaw CQLFn]))

(defn full-coll-from-change
  [{intsx :intersection
    added +}]
  (cond
    (empty? intsx) added
    (empty? added) intsx
    (map? intsx) (merge intsx added)
    :else (into intsx added)))

(defn combine-where
  [& clauses]
  (into []
        (->> clauses
             (filter identity)
             (apply concat))))

(s/defn select-statement
  "returns a Hayt select statement"

  ([table :- s/Keyword
    {:keys [limit columns] :as opts} :- sch/FullTableSelectOptsSchema]

   (h/select table
             (when columns (apply h/columns columns))
             (when limit (h/limit limit))))

  ([table :- s/Keyword
    key :- sch/KeySchema
    record-or-key-value :- sch/RecordOrKeyValueSchema
    {:keys [where columns order-by limit] :as opts} :- sch/SelectOptsSchema]
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

(s/defn placeholder-kw
  "a prepared-statement placeholder keyword"
  [prefix key]
  (->> key name (str (name prefix) "_") keyword))

(s/defn prepare-record-placeholders
  "replace the values in a record with prepared-statement placeholders"
  [prefix record]
  (->> record
       (map (fn [[k v]]
              [k (placeholder-kw prefix k)]))
       (into {})))

(s/defn prepare-record-values
  "replace the keys in a record with the prepared-statement placeholders"
  [prefix record]
  (->> record
       (map (fn [[k v]]
              [(placeholder-kw prefix k)
               (if (t/is-collection-column-diff? v)
                 (full-coll-from-change v)
                 v)]))
       (into {})))

(s/defn prepare-where-placeholders
  "replace the values in a where with preparead-statement placeholders"
  [where-clause]
  (->> where-clause
       (map (fn [[op col val]]
              [op col (placeholder-kw :where col)]))))

(s/defn prepare-where-values
  [where-clause]
  (->> where-clause
       (map (fn [[op col val]]
              [(placeholder-kw :where col) val]))
       (into {})))

(s/defn prepare-select-statement
  "return a hayt statement suitable for preparing a select query"
  ([table :- s/Keyword
    {:keys [where columns order-by limit] :as opts} :- sch/SelectOptsSchema]
   (let [where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-placeholders (prepare-where-placeholders where-clause)]
     (h/select table
               (when (not-empty where-placeholders)
                 (h/where where-placeholders))
               (when columns (apply h/columns columns))
               (when order-by (apply h/order-by order-by))
               (when limit (h/limit (placeholder-kw :limit :limit))))))

  ([table :- s/Keyword
    key :- sch/KeySchema
    record-or-key-value :- sch/RecordOrKeyValueSchema
    {:keys [where columns order-by limit] :as opts} :- sch/SelectOptsSchema]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
         where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-clause (combine-where key-clause where-clause)
         where-placeholders (prepare-where-placeholders where-clause)]
     (h/select table
               (when (not-empty where-placeholders)
                 (h/where where-placeholders))
               (when columns (apply h/columns columns))
               (when order-by (apply h/order-by order-by))
               (when limit (h/limit (placeholder-kw :limit :limit)))))))

(s/defn prepare-select-values
  "return values for use with a prepared statement"
  ([table :- s/Keyword
    {:keys [where columns order-by limit] :as opts} :- sch/SelectOptsSchema]
   (let [where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-values (prepare-where-values where-clause)
         ;; note the java driver requires ints for limit
         limit-value (when limit {(placeholder-kw :limit :limit) (int limit)})]
     (merge
      where-values
      limit-value)))

  ([table :- s/Keyword
    key :- sch/KeySchema
    record-or-key-value :- sch/RecordOrKeyValueSchema
    {:keys [where columns order-by limit] :as opts} :- sch/SelectOptsSchema]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
         where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-clause (combine-where key-clause where-clause)
         where-values (prepare-where-values where-clause)
         ;; note the java driver requires ints for limit
         limit-value (when limit {(placeholder-kw :limit :limit) (int limit)})]
     (merge
      where-values
      limit-value))))

(s/defn insert-statement
  "returns a Hayt insert statement"
  [table :- s/Keyword
   record :- sch/RecordSchema
   {:keys [if-not-exists using] :as opts} :- sch/InsertOptsSchema]
  (let [record-with-full-coll-cols (reduce
                                    (fn [rs [k v]]
                                      (if (t/is-collection-column-diff? v)
                                        (assoc rs k (full-coll-from-change v))
                                        rs))
                                    record
                                    record)]
    (h/insert table
             (h/values record-with-full-coll-cols)
             (when if-not-exists (h/if-not-exists true))
             (when (not-empty using) (apply h/using (flatten (seq using)))))))

(s/defn prepare-using-placeholders
  [using-clauses]
  (for [[k v] using-clauses]
    [k (placeholder-kw :using k)]))

(s/defn prepare-using-values
  [using-clauses]
  (->>
   (for [[k v] using-clauses]
     [(placeholder-kw :using k)
      (cond
        (= :ttl k) (int v) ;; java driver requires ttl int
        :else v)])
   (into {})))

(s/defn prepare-insert-statement
  "returns a Hayt prepared insert statement"
  [table :- s/Keyword
   record :- sch/RecordSchema
   {:keys [if-not-exists using] :as opts} :- sch/InsertOptsSchema]
  (let [insert-placeholders (prepare-record-placeholders :insert record)]

    (h/insert table
              (h/values insert-placeholders)
              (when if-not-exists (h/if-not-exists true))
              (when (not-empty using)
                (apply h/using (flatten (prepare-using-placeholders using)))))))

(s/defn prepare-insert-values
  [table :- s/Keyword
   record :- sch/RecordSchema
   {:keys [if-not-exists using] :as opts} :- sch/InsertOptsSchema]
  (let [insert-values (prepare-record-values :insert record)
        using-values (prepare-using-values using)]
    (merge insert-values
           using-values)))

(s/defn update-statement
  "returns a Hayt update statement"
  [table :- s/Keyword
   key :- sch/KeySchema
   record :- sch/RecordSchema
   {:keys [only-if
           if-exists
           if-not-exists
           using
           set-columns] :as opts} :- sch/UpdateOptsSchema]
  (let [key-clause (extract-key-equality-clause key record opts)
        raw-cols (if (not-empty set-columns)
                   (select-keys record set-columns)
                   (apply dissoc record (flatten-key key)))
        set-cols (reduce
                  (fn [rs [k v]]
                    (if (t/is-collection-column-diff? v)
                      (apply
                       conj
                       rs
                       (map
                        (fn [[op vs]] [k [op vs]])
                        (select-keys v [+ -])))
                      (conj rs [k v])))
                  []
                  raw-cols)

        stmt (h/update table
                       (h/set-columns set-cols)
                       (h/where key-clause)
                       (when only-if (h/only-if only-if))
                       (when if-exists (h/if-exists true))
                       (when if-not-exists (h/if-exists false))
                       (when (not-empty using) (apply h/using (flatten (seq using)))))]
    stmt))

(s/defn prepare-set-placeholders
  [set-cols]
  (reduce
   (fn [rs [k v]]
     (if (t/is-collection-column-diff? v)
       (conj
        rs
        [k [+ (placeholder-kw :set_add k)]]
        [k [- (placeholder-kw :set_rem k)]])
       (conj rs [k (placeholder-kw :set k)])))
   []
   set-cols))

(s/defn prepare-set-values
  [set-cols]
  (reduce
   (fn [rs [k v]]
     (if (t/is-collection-column-diff? v)
       (let [{added +
              removed -} v]
         (assoc
          rs
          (placeholder-kw :set_add k) added
          (placeholder-kw :set_rem k) removed))
       (assoc rs (placeholder-kw :set k) v)))
   {}
   set-cols))

(s/defn prepare-update-statement
  "returns a Hayt prepared update statement"
  [table :- s/Keyword
   key :- sch/KeySchema
   record :- sch/RecordSchema
   {:keys [only-if
           if-exists
           if-not-exists
           using
           set-columns] :as opts} :- sch/UpdateOptsSchema]
  (let [key-clause (extract-key-equality-clause key record opts)
        key-clause-placeholders (prepare-where-placeholders key-clause)
        set-cols (if (not-empty set-columns)
                   (select-keys record set-columns)
                   (apply dissoc record (flatten-key key)))
        set-placeholders (prepare-set-placeholders set-cols)

        stmt (h/update table
                       (h/set-columns set-placeholders)
                       (h/where key-clause-placeholders)
                       (when only-if (h/only-if only-if))
                       (when if-exists (h/if-exists true))
                       (when if-not-exists (h/if-exists false))
                       (when (not-empty using)
                         (apply h/using
                                (flatten (prepare-using-placeholders using)))))]
    stmt))

(s/defn prepare-update-values
  [table :- s/Keyword
   key :- sch/KeySchema
   record :- sch/RecordSchema
   {:keys [only-if
           if-exists
           if-not-exists
           using
           set-columns] :as opts} :- sch/UpdateOptsSchema]
  (let [key-clause (extract-key-equality-clause key record opts)
        key-clause-values (prepare-where-values key-clause)
        set-cols (if (not-empty set-columns)
                   (select-keys record set-columns)
                   (apply dissoc record (flatten-key key)))
        set-values (prepare-set-values set-cols)
        using-values (prepare-using-values using)]
    (merge
     key-clause-values
     set-values
     using-values)))

(s/defn delete-statement
  "returns a Hayt delete statement"
  [table :- s/Keyword
   key :- sch/KeySchema
   record-or-key-value :- sch/RecordOrKeyValueSchema
   {:keys [only-if if-exists using where] :as opts} :- sch/DeleteOptsSchema]
  (let [key-clause (extract-key-equality-clause key record-or-key-value opts)]
    (h/delete table
              (h/where (combine-where key-clause where))
              (when only-if (h/only-if only-if))
              (when if-exists (h/if-exists true))
              (when (not-empty using) (apply h/using (flatten (seq using)))))))

(s/defn prepare-delete-statement
  [table :- s/Keyword
   key :- sch/KeySchema
   record-or-key-value :- sch/RecordOrKeyValueSchema
   {:keys [only-if if-exists using where] :as opts} :- sch/DeleteOptsSchema]
  (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
        where-clause (combine-where key-clause where)
        where-placeholders (prepare-where-placeholders where-clause)
        using-placeholders (prepare-using-placeholders using)]
    (h/delete table
              (h/where where-placeholders)
              (when only-if (h/only-if only-if))
              (when if-exists (h/if-exists true))
              (when (not-empty using)
                (apply h/using (flatten using-placeholders))))))

(s/defn prepare-delete-values
  [table :- s/Keyword
   key :- sch/KeySchema
   record-or-key-value :- sch/RecordOrKeyValueSchema
   {:keys [only-if if-exists using where] :as opts} :- sch/DeleteOptsSchema]
  (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
        where-clause (combine-where key-clause where)
        where-values (prepare-where-values where-clause)
        using-values (prepare-using-values using)]
    (merge
     where-values
     using-values)))
