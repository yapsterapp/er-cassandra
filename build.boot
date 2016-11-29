(load-file "../build/boot_lib.clj")

(configure-minimal-env
 {:source-paths #{"src" "test"}
  :resource-paths  #{"dev-resources"}})

(run-embedded-cassandra)

(configure-yapster-repos)

(set-env!
 :dependencies
 (:dependencies (read-project "project.clj" [:test])))

(require
 '[adzerk.boot-test :refer [test]])
