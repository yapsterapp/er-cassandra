(ns er-cassandra.schema
  (:require
   [manifold.deferred :as d]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]))

(defn ^:private table-metadata-query
  [keyspace table]
  (h/->raw (h/select :system.schema_columnfamilies
                     (h/where {:keyspace_name keyspace
                               :columnfamily_name table}))))

(defn table-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  [session keyspace table]
  (d/chain (aliam/execute session (table-metadata-query keyspace table))
           first))

(defn ^:private column-metadata-query
  [keyspace table]
  (h/->raw (h/select :system.schema_columns
                     (h/where {:keyspace_name keyspace
                               :columnfamily_name table}))))

(defn column-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  [session keyspace table]
  (aliam/execute session (column-metadata-query keyspace table)))

(defn column-names
  [session keyspace table]
  (d/chain (column-metadata session keyspace table)
           #(map :column_name %)))
