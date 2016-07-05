(ns er-cassandra.schema
  (:require
   [environ.core :refer [env]]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [er-cassandra.session :as session])
  (:import
   [er_cassandra.session Session]))

;; HACK ALERT
(def cassandra-version (or (some-> (env :cassandra-version) Integer/parseInt)
                           2))

(defn ^:private table-metadata-query
  [keyspace table]
  (if (< cassandra-version 3)
    (h/select :system.schema_columnfamilies
              (h/where {:keyspace_name keyspace
                        :columnfamily_name table}))
    (h/select :system_schema.tables
              (h/where {:keyspace_name keyspace
                        :table_name table}))))

(defn table-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  [^Session session keyspace table]
  (d/chain (session/execute session (table-metadata-query keyspace table))
           first))

(defn ^:private usertype-metadata-query
  [keyspace type]
  (if (< cassandra-version 3)
    (h/select :system.schema_usertypes
              (h/where {:keyspace_name keyspace
                        :type_name type}))
    (h/select :system_schema.types
              (h/where {:keyspace_name keyspace
                        :type_name type}))))

(defn usertype-metadata
  [^Session session keyspace type]
  (d/chain (session/execute session (usertype-metadata-query keyspace type))
           first))

(defn ^:private column-metadata-query
  [keyspace table]
  (if (< cassandra-version 3)
    (h/select :system.schema_columns
              (h/where {:keyspace_name keyspace
                        :columnfamily_name table}))
    (h/select :system_schema.columns
              (h/where {:keyspace_name keyspace
                        :table_name table}))))

(defn column-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  [^Session session keyspace table]
  (session/execute session (column-metadata-query keyspace table)))

(defn column-names
  [^Session session keyspace table]
  (d/chain (column-metadata session keyspace table)
           #(map :column_name %)))
