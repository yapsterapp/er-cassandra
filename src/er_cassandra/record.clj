(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [plumbing.core :refer [assoc-when]]
   [taoensso.timbre :refer [trace debug info warn error]]
   [schema.core :as s]
   [clojure.set :as set]
   [manifold.deferred :as d]
   [manifold.stream :as stream]
   [qbits.hayt :as h]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :refer [flatten-key extract-key-equality-clause]]
   [er-cassandra.session :as session]
   [prpr.promise :as pr :refer [ddo]]
   [cats.context :refer [with-context]]
   [cats.core :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.record.schema :as sch]
   [er-cassandra.record.statement :as st])
  (:import
   [er_cassandra.session Session]
   [qbits.hayt.cql CQLRaw CQLFn]))

;; low-level record-based cassandra statement generation and execution
;;
;; keys can be extracted from records, provided explicitly or mixed.
;;
;; execution is async and returns a manifold Deferred

(defn select
  "select records"

  ([^Session session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {prepare? :prepare?
     :as opts}]
   (let [select-opts (select-keys opts [:columns :where :only-if :order-by :limit])
         select-stmt (if prepare?
                       (st/prepare-select-statement table
                                                 key
                                                 record-or-key-value
                                                 select-opts)
                       (st/select-statement table
                                         key
                                         record-or-key-value
                                         select-opts))
         ps-values (when prepare?
                     (st/prepare-select-values
                      table
                      key
                      record-or-key-value
                      select-opts))]
     ;; (warn "select-stmt" select-stmt)
     (session/execute
      session
      select-stmt
      (-> opts
          (dissoc :columns :where :only-if :order-by :limit)
          (assoc-when :values ps-values))))))

;; TODO change to return a Deferred<Stream> which turns out to be a lot more
;; convenient to work with

(defn select-buffered
  "select a stream of records

   if :buffer-size opt is given, a *downstream* buffer will be applied to
   the query stream. the query-buffer will be sized by the :fetch-size opt
   if given"

  ([^Session session table] (select-buffered session table {}))

  ([^Session session table {prepare? :prepare? :as opts}]
   (let [select-opts (select-keys opts [:columns :limit])
         select-stmt (if prepare?
                       (st/prepare-select-statement
                        table
                        select-opts)
                       (st/select-statement
                        table
                        select-opts))
         ps-values (when prepare?
                     (st/prepare-select-values
                      table
                      select-opts))]
     (session/execute-buffered
      session
      select-stmt
      (-> opts
          (dissoc :columns :limit)
          (assoc-when :values ps-values)))))

  ([^Session session table key record-or-key-value]
   (select session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {prepare? :prepare? :as opts}]
   (let [select-opts (select-keys opts [:columns :where :only-if :order-by :limit])
         select-stmt (if prepare?
                       (st/prepare-select-statement
                        table
                        key
                        record-or-key-value
                        select-opts)
                       (st/select-statement
                        table
                        key
                        record-or-key-value
                        select-opts))
         ps-values (when prepare?
                     (st/prepare-select-values
                      table
                      key
                      record-or-key-value
                      select-opts))]
     (session/execute-buffered
      session
      select-stmt
      (-> opts
          (dissoc :columns :where :only-if :order-by :limit)
          (assoc-when :values ps-values))))))

(defn select-one
  "select a single record"

  ([^Session session table key record-or-key-value]
   (select-one session table key record-or-key-value {}))

  ([^Session session table key record-or-key-value opts]
   (d/chain
    (select session table key record-or-key-value (merge opts {:limit 1}))
    first)))

(defn insert
  "insert a single record"

  ([^Session session table record]
   (insert session table record {}))

  ([^Session session table record opts]
   (d/chain
    (session/execute
     session
     (st/insert-statement
      table
      record
      (select-keys opts [:if-not-exists :using]))
     (dissoc opts :if-not-exists :using))
    first)))

(defn insert-buffered
  "insert a stream of records"
  ([^Session session table record-stream]
   (insert-buffered session
                    table
                    record-stream
                    {:buffer-size 25}))

  ([^Session session
    table
    record-stream
    {:keys [buffer-size] :as opts}]
   (->> record-stream
        (stream/map
         (fn [r]
           (insert session
                   table
                   r
                   (dissoc opts :buffer-size))))
        (stream/realize-each)
        ((fn [s]
           (if buffer-size
             (stream/buffer buffer-size s)
             s)))
        (return deferred-context))))

(defn update
  "update a single record"

  ([^Session session table key record]
   (update session table key record {}))

  ([^Session session table key record opts]
   (ddo [:let [stmt (st/update-statement
                     table
                     key
                     record
                     (select-keys opts [:only-if :if-exists :using :set-columns]))
               _ (warn stmt)]
         [resp _] (session/execute
                   session
                   stmt
                   (dissoc opts :only-if :if-exists :using :set-columns))]
     (return resp))))

(defn delete
  "delete a record"

  ([^Session session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([^Session session table key record-or-key-value opts]
   (d/chain
    (session/execute
     session
     (st/delete-statement
      table
      key
      record-or-key-value
      (select-keys opts [:only-if :if-exists :using :where]))
     (dissoc opts :only-if :if-exists :using :where))
    first)))
