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

                 [org.clojure/tools.reader "1.0.0-beta3"]
                 [org.clojure/tools.logging "0.3.1"]

                 ;; wow, such logging
                 [com.taoensso/timbre "4.10.0"]
                 [org.slf4j/slf4j-api "1.7.25"]

                 ;; JAR-HELL WARNING: slf4j-timbre has (needs) :aot :all
                 ;; which includes compiled version of a bunch of
                 ;; taoensso projects which will conflict
                 ;; with uncompiled versions required elsewhere
                 ;; unless slf4j-timbre is kept up to date
                 ;;
                 ;; org.clojure/clojure
                 ;; org.clojure/tools.reader
                 ;; org.slf4j/slf4j-api
                 ;; com.taoensso/timbre,
                 ;; com.taoensso/encore,
                 ;; com.taoensso/truss
                 ;; io.aviso/pretty
                  [employeerepublic/slf4j-timbre "0.4.3"]

                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]

                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [potemkin "0.4.3"
                  :exclusions [riddley]]
                 [prismatic/plumbing "0.5.4"]
                 [clj-time "0.13.0"]
                 [danlentz/clj-uuid "0.1.7"]
                 [cc.qbits/alia "4.0.0-beta4"]
                 [cc.qbits/alia-manifold "4.0.0-beta4"]
                 [cc.qbits/hayt "4.0.0"]
                 [environ "1.1.0"]
                 [drift "1.5.3"]
                 [manifold "0.1.6"]
                 [funcool/cats "2.1.0"]
                 [employeerepublic/deferst "0.5.0"]]

  :aliases {"test-repl" ["with-profile" "cassandra-unit,repl" "repl"]}

  :profiles {:repl {:pedantic? :ranges}

             :test {:resource-paths ["test-resources" "resources"]}})
