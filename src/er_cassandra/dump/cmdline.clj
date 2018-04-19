(ns er-cassandra.dump.cmdline
  (:require [cats.context :refer [with-context]]
            [cats.core :refer [mlet return]]
            [cats.labs.manifold :refer [deferred-context]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [manifold.deferred :as d]
            [plumbing.core :refer :all]))

(defnk fail-on-cli-errors!
  [errors missing-opts summary :as args]
  (if (or (not-empty errors)
          (not-empty missing-opts))
    (do
      (binding [*out* *err*]
        (doseq [err errors]
          (println err))
        (when (not-empty missing-opts)
          (println (str "Missing required options: " missing-opts)))
        (println "Docs:\n" summary))
      (d/error-deferred
       (ex-info "option parse failure" args)))
    (return args)))

(def common-cli-options
  [["-k" "--keyspace KEYSPACE" "Cassandra keyspace"]

   ["-h" "--host HOST" "Cassandra host"
    :default "localhost"]

   ["-p" "--port PORT" "Cassandra port"
    :parse-fn #(Integer/parseInt %)
    :default 9042]

   [nil "--skip TABLE,TABLE..." "list of tables to skip"
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (mapv keyword)))]

   ["-t" "--table TABLE" "a single table"]

   ["-d" "--directory FILE" "Output directory"]])

(def required-opts
  #{:keyspace
    :host
    :directory})

(defn parse-options
  "parse options and error if required options not provided"
  ([args] (parse-options nil args))
  ([additional-opts args]
   (with-context deferred-context
     (mlet [:let [{parsed-options :options
                   parsed-args :arguments
                   parse-errors :errors
                   summary :summary
                   :as parsed}  (parse-opts
                                 args
                                 (into common-cli-options additional-opts))]

            _ (fail-on-cli-errors!
               (assoc parsed
                      :missing-opts
                      (set/difference required-opts
                                      (set
                                       (keys parsed-options)))))]
       (return
        parsed)))))
