(ns er-cassandra.joplin.migration-helpers
  (:require [er-cassandra.session :as cass.session]))

(defmacro execute
  [session & body]
  `(deref
    (cass.session/execute ~session ~@body {})))

