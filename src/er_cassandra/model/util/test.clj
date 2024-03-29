(ns er-cassandra.model.util.test
  (:require
   [deferst.core :as deferst]
   [deferst.system :as sys]
   [er-cassandra.model :as m]
   [er-cassandra.model.alia.model-session :as ams]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.record :as r]
   [er-cassandra.session :as s]
   [prpr.stream :as stream]
   [slf4j-timbre.configure :as logconf]
   [taoensso.timbre :refer [error]]))

(def ^:dynamic *model-session* nil)

(def alia-test-model-session-config
  {:timbre {:level :warn}
   :config {:alia-session
            {:keyspace "er_cassandra_test"
             ;; set to false to preserve db contents after tests
             :truncate-on-close true
             ;; :trace? :warn
             ;; :consistency :all
             }}})

(def alia-test-model-session-system-def
  [[:logging logconf/configure-timbre [:timbre]]
   [:cassandra ams/create-test-session [:config :alia-session]]])

(defn with-system*
  [sys f]
  (try
    (let [system @(deferst/start! sys)]
      (binding [*model-session* (:cassandra system)]
        (f)))
    (finally
      (try
        @(deferst/stop! sys)
        (catch Exception e
          (error e "error during test stop-system!"))))))

(defn with-model-session-fixture
  "a clojure.test fixture which sets up logging and
   a :cassandra session"
  []
  (fn [f]

    (let [sb (sys/system-builder alia-test-model-session-system-def)
          sys (deferst/create-system
                sb
                alia-test-model-session-config)]
      (with-system* sys f))))

(defn create-table
  "creates a table for test - drops any existing version of the table first"
  [table-name table-def]
  @(s/execute
    *model-session*
    (str "drop table if exists " (name table-name))
    {})
  @(s/execute
    *model-session*
    (str "create table " (name table-name) " " table-def)
    {}))

(defn fetch-records
  [table key key-value]
  @(r/select *model-session* table key key-value))

(defn fetch-record
  [table key key-value]
  @(r/select-one *model-session* table key key-value))

(defn insert-record
  "insert records with a timestamp 1ms in the past by default"
  ([table record] (insert-record table record nil))
  ([table record opts]
   @(r/insert *model-session* table record (ts/past-timestamp-opt opts))))

(defn delete-record
  "delete a record with a timestamp 1ms in the past by default"
  ([table key key-value] (delete-record table key key-value {}))
  ([table key key-value opts]
   @(r/delete *model-session* table key key-value opts)))

(defn upsert-instance
  "upsert instances with a timestamp 1ms in the past by default"
  ([entity record] (upsert-instance entity record nil))
  ([entity record opts]
   @(m/select-upsert *model-session* entity record (ts/past-timestamp-opt opts))))

(defn fetch-instance
  ([entity key key-value] (fetch-instance entity key key-value {}))
  ([entity key key-value opts]
   @(m/select-one *model-session* entity key key-value opts)))

(defn record-stream
  ([table-name] (record-stream table-name {}))
  ([table-name opts]
   @(r/select-buffered *model-session* table-name opts)))

(defn instance-stream
  ([entity] (instance-stream entity {}))
  ([entity opts]
   @(m/select-buffered *model-session* entity opts)))

(defn upsert-instance-stream
  ([entity instance-stream]
   (upsert-instance-stream entity instance-stream {}))
  ([entity instance-stream opts]
   @(m/upsert-buffered *model-session*
                       entity
                       instance-stream
                       (ts/past-timestamp-opt opts))))

(defn sync-consume-stream
  "synchronously consume a stream"
  [stream]
  @(stream/reduce (fn [_ _]) nil stream))
