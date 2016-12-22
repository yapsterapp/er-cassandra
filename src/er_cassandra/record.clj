(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [plumbing.core :refer :all]
   [clojure.set :as set]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [qbits.hayt :as h]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :refer [flatten-key extract-key-equality-clause]]
   [er-cassandra.session :as session])
  (:import
   [er_cassandra.session Session]))

;; low-level record-based cassandra statement generation and execution
;;
;; keys can be extracted from records, provided explicitly or mixed.
;;
;; execution is async and returns a manifold Deferred

(defn check-opts
  [valid-opts opts]
  (let [uks (set/difference (set (keys opts)) valid-opts)]
    (when (not-empty uks)
      (throw (ex-info "unknown opts" {:unknown-opts uks
                                      :opts opts
                                      :valid-opts valid-opts})))))

(defn combine-where
  [& clauses]
  (into []
        (->> clauses
             (filter identity)
             (apply concat))))

(def select-opt-keys #{:where :columns :order-by :limit})

(defn select-statement
  "returns a Hayt select statement"

  ([table key record-or-key-value]
   (select-statement table key record-or-key-value {}))

  ([table
    key
    record-or-key-value
    {:keys [where columns order-by limit] :as opts}]
   (check-opts select-opt-keys opts)
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

  ([^Session session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {:keys [buffer-size] :as opts}]
   (let [strm (session/execute-buffered
               session
               (select-statement
                table
                key
                record-or-key-value
                (select-keys opts [:columns :where :only-if :order-by :limit]))
               (-> opts
                   (dissoc :columns :where :only-if :order-by :limit :buffer-size)))]
     (if buffer-size
       (s/buffer buffer-size strm)
       strm))))

(defn select-one
  "select a single record"

  ([^Session session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))

  ([^Session session table key record-or-key-value opts]
   (d/chain
    (select session table key record-or-key-value (merge opts {:limit 1}))
    first)))

(def insert-opt-keys #{:if-not-exists :using})

(defn insert-statement
  "returns a Hayt insert statement"

  ([table record] (insert-statement table record {}))

  ([table
    record
    {:keys [if-not-exists using] :as opts}]
   (check-opts insert-opt-keys opts)
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

(def update-opt-keys #{:only-if :if-exists :if-not-exists :using :set-columns})

(defn update-statement
  "returns a Hayt update statement"

  ([table key record] (update-statement table key record {}))

  ([table
    key
    record
    {:keys [only-if if-exists if-not-exists using set-columns] :as opts}]
   (check-opts update-opt-keys opts)
   (let [key-clause (extract-key-equality-clause key record opts)
         set-cols (if (not-empty set-columns)
                    (select-keys record set-columns)
                    (apply dissoc record (flatten-key key)))]
     (h/update table
               (h/set-columns set-cols)
               (h/where key-clause)
               (when only-if (h/only-if only-if))
               (when if-exists (h/if-exists true))
               (when if-not-exists (h/if-exists false))
               (when (not-empty using) (apply h/using (flatten (seq using))))))))

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

(def delete-opt-keys #{:only-if :if-exists :using :where})

(defn delete-statement
  "returns a Hayt delete statement"

  ([table key record-or-key-value]
   (delete-statement table key record-or-key-value {}))

  ([table
    key
    record-or-key-value
    {:keys [only-if if-exists using where] :as opts}]
   (check-opts delete-opt-keys opts)
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
