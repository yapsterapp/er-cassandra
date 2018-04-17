(ns er-cassandra.dump
  (:require
   [deferst.core :as deferst]
   [deferst.system :as sys]
   [cats.core :as monad :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.model.alia.model-session :as alia-session]
   [manifold.deferred :as d]
   [slf4j-timbre.configure :as logconfig]
   [taoensso.timbre :refer [info warn]]
   [er-cassandra.dump.cmdline :as cl]
   [er-cassandra.dump.tables :as tables])
  (:gen-class))

(defn system-config
  [{keyspace :keyspace
    host :host
    port :port
    datacenter :datacenter
    :as options}]
  {:config

   {:id "keyspace-exporter"
    :environment "production"
    :timbre {:level :info
             :environment "production"
             :opts {}}
    :alia-session
    {:contact-points [host]
     :datacenter datacenter
     :keyspace keyspace
     :port port
     ;; :trace? :warn
     :query-options {:consistency :local-one
                     :fetch-size 10000}
     :pooling-options {:max-connections-per-host
                       {:local 500
                        :remote 500}}
     }}})

(def system-builder
  (sys/system-builder
   [[:logging logconfig/configure-timbre [:config :timbre]]
    [:cassandra alia-session/create-session [:config :alia-session]]]))

(defn build-start-system
  [options]
  (let [sys (deferst/create-system
              system-builder
              (system-config options))]
    (deferst/start! sys)))

(defn exit
  [mv]
  (-> mv
      (d/chain (fn [_]
                 (System/exit 0)))
      (d/catch (fn [e]
                 (warn e "OOPS")
                 (System/exit 1)))))

(defn dump-keyspace
  [{cassandra :cassandra
    :as app}
   {keyspace :keyspace
    directory :directory
    skip :skip
    :as opts}]
  (tables/dump-tables
   cassandra
   keyspace
   directory
   skip))

(defn dump-table
  [{cassandra :cassandra
    :as app}
   {keyspace :keyspace
    directory :directory
    table :table
    :as opts}]
  (tables/dump-table
   cassandra
   keyspace
   directory
   table))

(defn load-keyspace
  [{cassandra :cassandra
    :as app}
   {keyspace :keyspace
    directory :directory
    skip :skip
    :as opts}]
  (tables/load-tables
   cassandra
   keyspace
   directory
   skip))

(defn load-table
  [{cassandra :cassandra
    :as app}
   {keyspace :keyspace
    directory :directory
    table :table
    :as opts}]
  (tables/load-table
   cassandra
   keyspace
   directory
   table))

(defn main
  [& args]
  (with-context deferred-context
    (mlet [{{keyspace :keyspace
             :as options} :options
            [cmd-sym] :arguments
            :as cmdline} (cl/parse-options args)

           :let [cmd-var (ns-resolve
                          'er-model.connectors.exporter.keyspace
                          (symbol cmd-sym))
                 _ (assert (some? cmd-var))]

           app (build-start-system options)]

      @(cmd-var
        app
        options))))

(defn -main
  [& args]
  (exit
   (apply main args)))
