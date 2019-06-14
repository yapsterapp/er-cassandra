(defproject employeerepublic/er-cassandra "_"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[lein-modules-bpk/lein-modules "0.3.13.bpk-20160816.002513-1"]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.reader
               org.clojure/tools.logging]

  :dependencies [[org.clojure/clojure "_"]
                 [org.clojure/tools.cli "_"]

                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [prismatic/plumbing "0.5.5"
                  :exclusions [prismatic/schema]]
                 [clj-time "0.15.1"]
                 [danlentz/clj-uuid "0.1.7"]
                 [cc.qbits/alia "4.3.0"
                  :exclusions [com.google.guava/guava]]
                 [cc.qbits/alia-manifold "4.3.0"
                  :exclusions [manifold]]
                 [cc.qbits/hayt "4.0.2"]
                 [environ "1.1.0"]
                 [drift "1.5.3"
                  :exclusions [org.clojure/clojure
                               org.clojure/tools.logging
                               org.clojure/java.classpath]]
                 [joplin.core "0.3.11"]
                 [joplin.cassandra "0.3.11"]
                 [com.cognitect/transit-clj "0.8.313"]
                 [employeerepublic/promisespromises "_"]]

  :aliases {"test-repl" ["with-profile" "cassandra-unit,repl" "repl"]}

  :profiles {:repl {:pedantic? :ranges}

             :test {:resource-paths ["test-resources" "resources"]}

             :uberjar {:aot [er-cassandra.dump]
                       :uberjar-name "cassandra.jar"
                       :global-vars {*assert* false}}})
