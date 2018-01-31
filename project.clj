(defproject employeerepublic/er-cassandra "0.11"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.reader
               org.clojure/tools.logging]

  :dependencies [[org.clojure/clojure "1.8.0"]

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
                 [employeerepublic/deferst "0.5.0"
                  :exclusions [funcool/promesa
                               manifold]]
                 [employeerepublic/promisespromises "_"]]

  :aliases {"test-repl" ["with-profile" "cassandra-unit,repl" "repl"]}

  :profiles {:repl {:pedantic? :ranges}

             :test {:resource-paths ["test-resources" "resources"]}})
