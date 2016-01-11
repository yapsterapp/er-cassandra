(defproject employeerepublic/er-cassandra "0.8.0"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :pedantic? :abort

  :dependencies [[org.clojure/tools.cli "0.3.3"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/core.match "0.3.0-alpha4"
                  :exclusions [org.clojure/tools.analyzer.jvm]]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [potemkin "0.4.3"]
                 [prismatic/plumbing "0.5.2"]
                 [clj-time "0.11.0"
                  :exclusions [org.clojure/clojure]]
                 [cc.qbits/alia "3.0.0-beta1"
                  :exclusions [org.clojure/clojure]] ;; :exclusions [com.google.guava/guava]
                 [environ "1.0.1"
                  :exclusions [org.clojure/clojure]]
                 [drift "1.5.3"
                  :exclusions [org.clojure/clojure
                               org.clojure/tools.logging]]
                 [manifold "0.1.2-alpha2"]
                 [funcool/cats "1.2.1"]]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]]
                   :plugins []}})
