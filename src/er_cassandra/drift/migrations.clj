(ns er-cassandra.drift.migrations
  (:require
   [clojure.string :as str]
   [clojure.tools.cli :refer [cli]]
   [clojure.tools.logging :as log]
   [qbits.hayt :as h]
   [drift.config]
   [er-cassandra.session :as session]
   [er-cassandra.schema :as schema])
  (:import
   [er_cassandra.session Session]))

(def ^:private migrations-table-name "schema_migrations")

(defn- create-migration-table-query
  []
  (h/create-table migrations-table-name
                  (h/column-definitions
                   {:namespace :varchar
                    :version :bigint
                    :primary-key [:namespace :version]})))

(defn- create-migration-table
  "create a schema_migrations table if it doesn't already exist"
  [^Session session keyspace]
  (when-not @(schema/table-metadata
              session
              keyspace
              migrations-table-name)
    (log/infof "creating migrations table: %s" migrations-table-name)
    (deref (session/execute
            session
            (create-migration-table-query)))))

(defn- namespace-versions-query
  [namespace]
  (h/select migrations-table-name
            (h/columns :version)
            (h/where {:namespace namespace})))

(defn- namespace-max-version
  [^Session session namespace]
  (->> (namespace-versions-query namespace)
       (session/execute session)
       deref
       (map :version)
       sort
       last))

(defn- delete-namespace-version-query
  [namespace version]
  (h/delete migrations-table-name
            (h/where {:namespace namespace
                      :version version})))

(defn- delete-namespace-version
  [^Session session namespace version]
  (deref
   (session/execute session (delete-namespace-version-query namespace version))))

(defn- namespace-versions-above-query
  [namespace version]
  (h/select migrations-table-name
            (h/columns :version)
            (h/where [[= :namespace namespace]
                      [> :version version]])))

(defn- namespace-versions-above
  [^Session session namespace version]
  (->> (namespace-versions-above-query namespace version)
       (session/execute session)
       deref
       (map :version)))

(defn- delete-namespace-versions-above
  [^Session session namespace version]
  (->> (namespace-versions-above session namespace version)
       (map (fn [v] (delete-namespace-version session namespace v)))
       dorun))


(defn- insert-namespace-version-query
  [^Session namespace version]
  (h/insert migrations-table-name
            (h/values {:namespace namespace
                       :version version})))

(defn- insert-namespace-version
  [^Session session namespace version]
  (delete-namespace-versions-above session namespace version)
  (if (> version 0)
    (deref
     (session/execute
      session
      (insert-namespace-version-query namespace version)))))

(defn create-init-fn
  "create a drift init function"
  ([^Session session keyspace config-namespace]
   (create-init-fn session keyspace config-namespace {}))
  ([^Session session keyspace config-namespace other-config]
   (fn init
     [args]
     (let [[opts oargs usage] (cli
                               args
                               ["-p" "--prefix" "model prefix"])
           prefix (:prefix opts)
           namespace (->> [prefix config-namespace]
                          (filter identity)
                          (str/join "__"))]

       ;; (log/info (pr-str ["INIT-FN" session keyspace config-namespace]))

       (create-migration-table session keyspace)

       (log/infof "migrating schema for prefix: %s)" (or prefix "<no prefix>"))
       (merge
        other-config
        {:er-cassandra-session session
         :cassandra-keyspace keyspace
         :namespace namespace
         :prefix prefix})))))

(defn current-version
  "drift fn - get the current version"
  []
  (let [config drift.config/*config-map*
        namespace (:namespace config)
        session (:er-cassandra-session config)]

    (log/infof "config: %s" (prn-str config))
    (or (namespace-max-version session namespace)
        0)))

(defn update-version
  "drift fn - update the version"
  [version]
  (let [config drift.config/*config-map*
        namespace (:namespace config)
        session (:er-cassandra-session config)]
    (insert-namespace-version session namespace version)))

(defn finished
  "kill the VM when finished"
  []
  (let [config drift.config/*config-map*]
    (when-not (:remain-when-finished config)
      (System/exit 0))))

(def ns-content
  (str "\n  (:require [qbits.alia :as alia]"
       "\n            [qbits.hayt :as h]"
       "\n            [er-cassandra.drift.migration-helpers :refer :all])"))
