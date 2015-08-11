(ns er-cassandra.record-either
  (:refer-clojure :exclude [update])
  (:require
   [potemkin :refer [import-vars]]
   [cats.core :as m]
   [cats.monad.either :as either]
   [cats.monad.deferred :as dm]
   [er-cassandra.record :as r]))

;; low-level record-based cassandra interface which
;; puts responses into an Either monad Right value,
;; and transforms Exceptions into Left values

(defn alia-error-transformer
  "transforms an alia exception into an error map... doesn't
   do much more than extract the ex-data map and set a message"
  [x]
  (let [data (if (instance? clojure.lang.ExceptionInfo x)
               (ex-data x)
               {:type (-> x type .getName keyword)})]
    (merge {:message (.getMessage x)
            :exception x}
           data)))

(import-vars [er-cassandra.record select-statement])

(defn select
  "select records : returns a Deferred[Either]"
  ([session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (dm/either-deferred
    (r/select session table key record-or-key-value opts)
    alia-error-transformer)))

(defn select-one
  "select a single record : returns a Deferred[Either]"
  ([session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (dm/either-deferred
    (r/select-one session table key record-or-key-value opts)
    alia-error-transformer)))

(import-vars [er-cassandra.record insert-statement])

(defn insert
  "insert a single record - returns a Deferred[Either]"

  ([session table record]
   (insert session table record {}))

  ([session table record opts]
   (dm/either-deferred
    (r/insert session table record opts)
    alia-error-transformer)))

(import-vars [er-cassandra.record update-statement])

(defn update
  "update a single record - returns a Deferred[Either]"

  ([session table key record]
   (update session table key record {}))

  ([session table key record opts]
   (dm/either-deferred
    (r/update session table key record opts)
    alia-error-transformer)))

(import-vars [er-cassandra.record delete-statement])

(defn delete
  "delete a record - returns a Deferred[Either]"

  ([session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([session table key record-or-key-value opts]
   (dm/either-deferred
    (r/delete session table key record-or-key-value opts)
    alia-error-transformer)))
