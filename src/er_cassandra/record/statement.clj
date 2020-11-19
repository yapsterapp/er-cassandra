(ns er-cassandra.record.statement
  (:require
   [schema.core :as s]
   [qbits.hayt :as h]
   [er-cassandra.key :refer [flatten-key extract-key-equality-clause]]
   [er-cassandra.model.types :as t]
   [er-cassandra.record.schema :as sch])
  (:import
   [qbits.hayt.cql CQLRaw CQLFn]))

(s/defn placeholder-kw
  "a prepared-statement placeholder keyword"
  [prefix key]
  (->> key name (str (name prefix) "_") keyword))

(defn full-coll-from-change
  [{intsx :intersection
    prepended :prepended
    appended :appended}]
  (reduce into [intsx prepended appended]))

(defn coll-col-diff->update
  ([rs k ccd]
   (coll-col-diff->update rs k ccd false))
  ([rs
    k
    {prpnd :prepended
     appnd :appended
     remvd :removed
     :as ccd}
    ;; I don't like the fact that these options only differ by a single char 😬
    {prepare? :prepare?
     prepared? :prepared?}]
   (let [has-new-elems? (boolean (or (seq prpnd) (seq appnd)))
         has-removed-elems? (boolean (seq remvd))
         prpnd-prep-kw (placeholder-kw :set_prepend k)
         appnd-prep-kw (placeholder-kw :set_append k)
         set-prep-kw (placeholder-kw :set k)]
     (if (and has-new-elems? (not has-removed-elems?))
       (cond-> rs
         (seq prpnd) (conj
                      (cond
                        prepared? [prpnd-prep-kw prpnd]
                        prepare?  [k [prpnd-prep-kw +]]
                        :else     [k [prpnd +]]))
         (seq appnd) (conj
                      (cond
                        prepared? [appnd-prep-kw appnd]
                        prepare?  [k [+ appnd-prep-kw]]
                        :else     [k [+ appnd]])))
       (conj
        rs
        (cond
          prepared? [set-prep-kw (full-coll-from-change ccd)]
          prepare?  [k set-prep-kw]
          :else     [k (full-coll-from-change ccd)]))))))

(defn combine-where
  [& clauses]
  (into []
        (->> clauses
             (filter identity)
             (apply concat))))

(s/defn select-statement
  "returns a Hayt select statement"

  ([table :- s/Keyword
    {:keys [where limit columns allow-filtering] :as opts} :- sch/FullTableSelectOptsSchema]

   (let [where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))]

     (h/select table
               (when where-clause (h/where where-clause))
               (when columns (apply h/columns columns))
               (when limit (h/limit limit))
               (when (some? allow-filtering) (h/allow-filtering allow-filtering)))))

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
                      (coll-col-diff->update rs k v)
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
       (coll-col-diff->update rs k v {:prepare? true})
       (conj rs [k (placeholder-kw :set k)])))
   []
   set-cols))

(s/defn prepare-set-values
  [set-cols]
  (reduce
   (fn [rs [k v]]
     (if (t/is-collection-column-diff? v)
       (coll-col-diff->update rs k v {:prepared? true})
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
