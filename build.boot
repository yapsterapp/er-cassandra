;; use lein to parse the project.clj for the deps
;; since lein-modules merges from the hierarchy
(require '[boot.pod :as pod])

(def lein-pod (pod/make-pod
               (update-in (get-env) [:dependencies]
                          conj '[leiningen "2.7.1"])))

(defn cassandra-unit-proj
  []
  (pod/with-eval-in lein-pod
    (require '[leiningen.core.project :as p])
    (p/read "project.clj" [:cassandra-unit])))

(def cass-deps (:dependencies (cassandra-unit-proj)))

(def cass-pod (pod/make-pod
               (update-in (get-env) [:dependencies]
                          concat cass-deps)))

(pod/with-eval-in cass-pod
  (import '[org.cassandraunit.utils EmbeddedCassandraServerHelper])
  (EmbeddedCassandraServerHelper/startEmbeddedCassandra))

(defn test-proj
  []
  (pod/with-eval-in lein-pod
    (require '[leiningen.core.project :as p])
    (p/read "project.clj" [:test])))

(def test-deps (:dependencies (test-proj)))
(set-env!
 :wagons       '[[s3-wagon-private "1.2.0"]]

 :repositories #(concat % [["releases" {:url "s3p://yapster-s3-wagon/releases/"
                                        :username [:env/aws_access_key :gpg]
                                        :passphrase [:env/aws_secret_key :gpg]}]
                           ["snapshots" {:url "s3p://yapster-s3-wagon/snapshots/"
                                         :username [:env/aws_access_key :gpg]
                                         :passphrase [:env/aws_secret_key :gpg]}]])

 :dependencies test-deps

 :source-paths #{"src" "test"})

(require
 '[adzerk.boot-test :refer [test]])

;; set system-property for embedded test
(System/setProperty "cassandra_port" "9142")
