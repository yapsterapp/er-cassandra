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

                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [potemkin "0.4.3"
                  :exclusions [riddley]]
                 [prismatic/plumbing "0.5.5"]
                 [clj-time "0.13.0"]
                 [danlentz/clj-uuid "0.1.7"]
                 [mccraigmccraig/alia "4.0.4"
                  :exclusions [com.google.guava/guava]]
                 [cc.qbits/alia-manifold "4.0.3"
                  :exclusions [manifold]]
                 [cc.qbits/hayt "4.0.0"]
                 [environ "1.1.0"]
                 [drift "1.5.3"]
                 [employeerepublic/deferst "0.6.0"
                  :exclusions [funcool/promesa
                               manifold]]
                 [com.cognitect/transit-clj "0.8.297"]
                 [employeerepublic/promisespromises "_"]]

  :aliases {"test-repl" ["with-profile" "cassandra-unit,repl" "repl"]}

  :profiles {:repl {:pedantic? :ranges}

             :test {:resource-paths ["test-resources" "resources"]}

             :uberjar {:aot [er-cassandra.dump]
                       :uberjar-name "cassandra.jar"
                       :global-vars {*assert* false}}})
