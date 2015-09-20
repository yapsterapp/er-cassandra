(defproject employeerepublic/er-cassandra "0.4.9"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/tools.cli "0.3.2"]
                 [org.clojure/core.match "0.2.2"]
                 [potemkin "0.4.1"]
                 [prismatic/plumbing "0.4.4"]
                 [clj-time "0.10.0"]
                 [cc.qbits/alia "2.7.3" ] ;; :exclusions [com.google.guava/guava]
                 [drift "1.5.3" :exclusions [org.clojure/clojure]]
                 [mccraigmccraig/cats "1.1.0-20150920.154602-1"]]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [expectations "2.1.2" :exclusions [com.google.guava/guava]]]

                   :plugins [[lein-expectations "0.0.8"]]}})
