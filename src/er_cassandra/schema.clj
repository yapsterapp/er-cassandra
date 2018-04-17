(ns er-cassandra.schema
  (:require
   [environ.core :refer [env]]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [er-cassandra.session :as session])
  (:import
   [er_cassandra.session Session]))

(defn ^:private table-metadata-query
  [keyspace table]
  (h/select :system_schema.tables
            (h/where {:keyspace_name keyspace
                      :table_name table})))

(defn table-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  ([^Session session table]
   (table-metadata session (session/keyspace session) table))
  ([^Session session keyspace table]
   (d/chain (session/execute session
                             (table-metadata-query
                              (name keyspace)
                              (name table))
                             {})
            first)))

(defn ^:private table-columns-query
  [keyspace table]
  (h/select :system_schema.columns
            (h/where {:keyspace_name keyspace
                      :table_name table})))

(defn table-columns
  ([^Session session table]
   (table-columns session (session/keyspace session) table))
  ([^Session session keyspace table]
   (session/execute session
                    (table-columns-query
                     (name keyspace)
                     (name table))
                    {})))

(defn ^:private usertype-metadata-query
  [keyspace type]
  (h/select :system_schema.types
            (h/where {:keyspace_name keyspace
                      :type_name type})))

(defn usertype-metadata
  ([^Session session type]
   (usertype-metadata session (session/keyspace session) type))
  ([^Session session keyspace type]
   (d/chain (session/execute session
                             (usertype-metadata-query
                              (name keyspace)
                              (name type))
                             {})
            first)))

(defn ^:private column-metadata-query
  [keyspace table]
  (h/select :system_schema.columns
            (h/where {:keyspace_name keyspace
                      :table_name table})))

(defn column-metadata
  "returns a Deferred with metadata for the requested table (or nil)"
  ([^Session session table]
   (column-metadata session (session/keyspace session) table))
  ([^Session session keyspace table]
   (session/execute session
                    (column-metadata-query
                     (name keyspace)
                     (name table))
                    {})))

(defn column-names
  ([^Session session table]
   (column-names session (session/keyspace session) table))
  ([^Session session keyspace table]
   (d/chain (column-metadata session keyspace table)
            #(map :column_name %))))
