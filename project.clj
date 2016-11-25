(defproject employeerepublic/er-cassandra "_"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[lein-modules-bpk/lein-modules "0.3.13.bpk-SNAPSHOT"]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.reader
               org.clojure/tools.logging]

  :dependencies [[org.clojure/tools.reader "1.0.0-beta3"]
                 [org.clojure/tools.logging "0.3.1"]

                 ;; wow, such logging
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.slf4j/slf4j-api "1.7.21"]
                 [org.slf4j/jcl-over-slf4j "1.7.21"]
                 [org.slf4j/log4j-over-slf4j "1.7.21"]
                 [org.slf4j/jul-to-slf4j "1.7.21"]
                 [com.taoensso/timbre "4.7.3"]

                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.3"]
                 [potemkin "0.4.3"]
                 [prismatic/plumbing "0.5.3"]
                 [clj-time "0.12.0"]
                 [cc.qbits/alia "3.2.0"]
                 [cc.qbits/alia-manifold "3.2.0"]
                 [cc.qbits/hayt "3.2.0"]
                 [environ "1.1.0"]
                 [drift "1.5.3"]
                 [manifold "0.1.5"]
                 [funcool/cats "2.0.0"]]

  :aliases {"test-repl" ["with-profile" "cassandra-unit,repl" "repl"]}

  :profiles {:repl {:pedantic? :ranges
                    :dependencies [[org.clojure/clojure "_"]]}

             :cassandra-unit
             {:dependencies
              [[org.cassandraunit/cassandra-unit "3.0.0.1"]]}
             })
