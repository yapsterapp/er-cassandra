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

(defn ^:private table-columns-metadata-query
  [keyspace table]
  (h/select :system_schema.columns
            (h/where {:keyspace_name keyspace
                      :table_name table})))

(defn table-columns-metadata
  "metadata for all columns of a table"
  ([^Session session table]
   (table-columns-metadata session (session/keyspace session) table))
  ([^Session session keyspace table]
   (session/execute session
                    (table-columns-metadata-query
                     (name keyspace)
                     (name table))
                    {})))

(defn ^:private column-metadata-query
  [keyspace table column]
  (h/select :system_schema.columns
            (h/where {:keyspace_name keyspace
                      :table_name table
                      :column_name column})))

(defn column-metadata
  "metadata for a single column"
  ([^Session session table column]
   (column-metadata session (session/keyspace session) table column))
  ([^Session session keyspace table column]
   (d/chain
    (session/execute session
                     (column-metadata-query
                      (name keyspace)
                      (name table)
                      (name column))
                     {})
    first)))

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

(defn ^:private view-metadata-query
  [keyspace view]
  (h/select :system_schema.views
            (h/where {:keyspace_name keyspace
                      :view_name view})))

(defn view-metadata
  "returns a Deferred with metadata for the requested view (or nil)"
  ([^Session session view]
   (view-metadata session (session/keyspace session) view))
  ([^Session session keyspace view]
   (d/chain (session/execute session
                             (view-metadata-query
                              (name keyspace)
                              (name view))
                             {})
            first)))

(defn view-columns-metadata
  "metadata for all columns of a view"
  ([^Session session view]
   (view-columns-metadata session (session/keyspace session) view))
  ([^Session session keyspace view]
   (session/execute session
                    (table-columns-metadata-query
                     (name keyspace)
                     (name view))
                    {})))

