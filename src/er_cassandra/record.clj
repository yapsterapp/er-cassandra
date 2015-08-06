(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [plumbing.core :refer :all]
   [qbits.alia.manifold :as aliam]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [clj-uuid :as uuid]
   [er-cassandra.key :refer [make-sequential extract-key-equality-clause]]))

(defn select-statement
  "returns a Hayt select statement"

  ([table key record-or-key-value]
   (select-statement table key record-or-key-value {}))

  ([table key record-or-key-value opts]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)]
     (h/select table
               (h/where key-clause)))))

(defn select
  "select records"

  ([session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (aliam/execute
    session
    (h/->raw
     (select-statement table key record-or-key-value opts)))))

(defn select-one
  "select a single record"
  ([session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))
  ([session table key record-or-key-value opts]
   (d/chain (select session table key record-or-key-value opts)
            first)))

(defn insert-statement
  "returns a Hayt insert statement"

  ([table key record] (insert-statement table key record {}))

  ([table
    key
    record
    {:keys [if-not-exists using] :as opts}]
   (h/insert table
             (h/values record)
             (when if-not-exists (h/if-not-exists true))
             (when (not-empty using) (apply h/using (flatten (seq using)))))))

(defn insert
  "insert a single record"

  ([session table key record]
   (insert session table key record {}))

  ([session table key record opts]
   (aliam/execute
    session
    (h/->raw (insert-statement table key record opts)))))

(defn update-statement
  "returns a Hayt update statement"

  ([table key record] (update-statement table key record {}))

  ([table
    key
    record
    {:keys [only-if if-exists using] :as opts}]
   (let [key-clause (extract-key-equality-clause key record opts)
         set-cols (apply dissoc record (make-sequential key))]
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
   (aliam/execute
    session
    (h/->raw (update-statement table key record opts)))))

(defn delete-statement
  "returns a Hayt delete statement"

  ([table key record-or-key-value]
   (delete-statement table key record-or-key-value {}))

  ([table
    key
    record-or-key-value
    {:keys [only-if if-exists using] :as opts}]
   (let [key-clause (extract-key-equality-clause key record-or-key-value opts)]
     (h/delete table
               (h/where key-clause)
               (when only-if (h/only-if only-if))
               (when if-exists (h/if-exists true))
               (when (not-empty using) (apply h/using (flatten (seq using))))))))

(defn delete
  "delete a record"

  ([session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (aliam/execute
    session
    (h/->raw
     (delete-statement table key record-or-key-value opts)))))
