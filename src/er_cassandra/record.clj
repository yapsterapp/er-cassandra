(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [plumbing.core :refer :all]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [er-cassandra.key :refer [make-sequential extract-key-equality-clause]]
   [er-cassandra.session :as session]))

;; low-level record-based cassandra statement generation and execution
;;
;; keys can be extracted from records, provided explicitly or mixed.
;;
;; execution is async and returns a manifold Deferred

(defn select-statement
  "returns a Hayt select statement"

  ([table key record-or-key-value]
   (select-statement table key record-or-key-value {}))

  ([table
    key
    record-or-key-value
    {:keys [columns where only-if order-by limit] :as opts}]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)
         where-clause (if (sequential? (first where))
                        where ;; it's already a seq of conditions
                        (when (not-empty where) [where]))
         where-clause (into key-clause where-clause)]
     (h/select table
               (h/where where-clause)
               (when columns (apply h/columns columns))
               (when only-if (h/only-if only-if))
               (when order-by (h/order-by order-by))
               (when limit (h/limit limit))))))

(defn select
  "select records"

  ([session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (session/execute
    session
    (select-statement table key record-or-key-value opts))))

(defn select-one
  "select a single record"

  ([session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (d/chain (select session table key record-or-key-value (merge opts {:limit 1}))
            first)))

(defn insert-statement
  "returns a Hayt insert statement"

  ([table record] (insert-statement table record {}))

  ([table
    record
    {:keys [if-not-exists using] :as opts}]
   (h/insert table
             (h/values record)
             (when if-not-exists (h/if-not-exists true))
             (when (not-empty using) (apply h/using (flatten (seq using)))))))

(defn insert
  "insert a single record"

  ([session table record]
   (insert session table record {}))

  ([session table record opts]
   (d/chain
    (session/execute
     session
     (insert-statement table record opts))
    first)))

(defn update-statement
  "returns a Hayt update statement"

  ([table key record] (update-statement table key record {}))

  ([table
    key
    record
    {:keys [only-if if-exists using set-columns] :as opts}]
   (let [key-clause (extract-key-equality-clause key record opts)
         set-cols (if (not-empty set-columns)
                    (select-keys record set-columns)
                    (apply dissoc record (make-sequential key)))]
     (h/update table
               (h/set-columns set-cols)
               (h/where key-clause)
               (when only-if (h/only-if only-if))
               (when if-exists (h/if-exists true))
               (when (not-empty using) (apply h/using (flatten (seq using))))))))

(defn update
  "update a single record"

  ([session table key record]
   (update session table key record {}))

  ([session table key record opts]
   (d/chain
    (session/execute
     session
     (update-statement table key record opts))
    first)))

(defn combine-where
  [& clauses]
  (into []
        (->> clauses
             (filter identity)
             (apply concat))))

(defn delete-statement
  "returns a Hayt delete statement"

  ([table key record-or-key-value]
   (delete-statement table key record-or-key-value {}))

  ([table
    key
    record-or-key-value
    {:keys [only-if if-exists using where] :as opts}]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)]
     (h/delete table
               (h/where (combine-where key-clause where))
               (when only-if (h/only-if only-if))
               (when if-exists (h/if-exists true))
               (when (not-empty using) (apply h/using (flatten (seq using))))))))

(defn delete
  "delete a record"

  ([session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (d/chain
    (session/execute
     session
     (delete-statement table key record-or-key-value opts))
    first)))
