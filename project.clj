(defproject employeerepublic/er-cassandra "0.10.0-SNAPSHOT"
  :description "a simple cassandra conector"
  :url "https://github.com/employeerepublic/er-cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :repositories [["releases" {:url "s3p://yapster-s3-wagon/releases/"
                              :username [:gpg :env/aws_access_key]
                              :passphrase [:gpg :env/aws_secret_key]}]
                 ["snapshots" {:url "s3p://yapster-s3-wagon/snapshots/"
                               :username [:gpg :env/aws_access_key]
                               :passphrase [:gpg :env/aws_secret_key]}]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.reader
               org.clojure/tools.logging]

  :dependencies [;; this lot just for logging
                 [org.clojure/tools.reader "1.0.0-beta1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.5"]
                 [org.slf4j/slf4j-api "1.7.18"]
                 [org.slf4j/jcl-over-slf4j "1.7.18"]
                 [org.slf4j/log4j-over-slf4j "1.7.18"]
                 [org.slf4j/jul-to-slf4j "1.7.18"]
                 [com.taoensso/timbre "4.3.1"]

                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [potemkin "0.4.3"]
                 [prismatic/plumbing "0.5.3"]
                 [clj-time "0.12.0"]
                 [cc.qbits/alia "3.1.4"]
                 [cc.qbits/alia-manifold "3.1.3"]
                 [cc.qbits/hayt "3.0.1"]
                 [environ "1.0.2"]
                 [drift "1.5.3"]
                 [manifold "0.1.4"]
                 [funcool/cats "1.2.1"]]

  :plugins [[s3-wagon-private "1.2.0"
             :exclusions [commons-codec]]
            [commons-codec "1.4"]]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]]
                   :plugins []}})
