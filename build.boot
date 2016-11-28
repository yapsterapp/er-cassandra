;; use lein to parse the project.clj for the deps
;; since lein-modules merges from the hierarchy
(require '[boot.pod :as pod])

(def lein-pod (pod/make-pod
               (update-in (get-env) [:dependencies]
                          conj '[leiningen "2.7.1"])))

(def lein-proj (pod/with-eval-in lein-pod
                 (require '[leiningen.core.project :as p])
                 (p/read)))

(def deps (:dependencies lein-proj))

(set-env!
 :wagons       '[[s3-wagon-private "1.2.0"]]

 :repositories #(concat % [["releases" {:url "s3p://yapster-s3-wagon/releases/"
                                        :username [:env/aws_access_key :gpg]
                                        :passphrase [:env/aws_secret_key :gpg]}]
                           ["snapshots" {:url "s3p://yapster-s3-wagon/snapshots/"
                                         :username [:env/aws_access_key :gpg]
                                         :passphrase [:env/aws_secret_key :gpg]}]])

 :dependencies deps

 :source-paths #{"src" "test"})

(require
 '[adzerk.boot-test :refer :all])
