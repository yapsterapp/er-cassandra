(ns er-cassandra.util.test
  (:require
   [clojure.test :as t]
   [taoensso.timbre :as timbre
    :refer [trace debug info warn error]]
   [deferst.core :as deferst :refer [defsystem]]
   [deferst.system :as sys]
   [er-cassandra.session :as s]
   [er-cassandra.session.alia :as alia-session]))

(def ^:dynamic *session* nil)

(def alia-test-session-config
  {:timbre {:level :warn}
   :config {:alia-session
            {:keyspace "er_cassandra_test"
             ;; set to false to preserve db contents after test
             :truncate-on-close true}}})

(defn configure-timbre
  [_]
  (timbre/set-config!
   (assoc timbre/example-config
          :level :warn)))

(def alia-test-session-system-def
  [[:logging configure-timbre {}]
   [:cassandra
    alia-session/create-test-session
    [:config :alia-session]]])

(defn with-session-fixture
  []
  (fn [f]

    (let [sb (sys/system-builder alia-test-session-system-def)
          sys (sys/start-system! sb alia-test-session-config)]
      (try
        (let [system @(sys/system-map sys)]
          (binding [*session* (:cassandra system)]
            (f)))
        (finally
          (try
            @(sys/stop-system! sys)
            (catch Exception e
              (error e "error during test stop-system!"))))))))

(defn create-table
  "creates a table for test - drops any existing version of the table first"
  [table-name table-def]
  @(s/execute
    *session*
    (str "drop table if exists " (name table-name))
    {})
  @(s/execute
    *session*
    (str "create table " (name table-name) " " table-def)
    {}))
