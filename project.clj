(defproject employeerepublic/er-cassandra "0.7.3"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/tools.cli "0.3.2"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [potemkin "0.4.1"]
                 [prismatic/plumbing "0.4.4"]
                 [clj-time "0.10.0"]
                 [cc.qbits/alia "2.12.0"] ;; :exclusions [com.google.guava/guava]
                 [environ "1.0.0"]
                 [drift "1.5.3" :exclusions [org.clojure/clojure]]
                 [manifold "0.1.1"]
                 [mccraigmccraig/cats "1.1.0-20150920.154602-1"]]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [expectations "2.1.2" :exclusions [com.google.guava/guava]]]

                   :plugins [[lein-expectations "0.0.8"]]}})
