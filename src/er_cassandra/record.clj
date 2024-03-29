(ns er-cassandra.record
  (:refer-clojure :exclude [update])
  (:require
   [cats.core :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [er-cassandra.record.sorted-stream :as r.ss]
   [er-cassandra.record.statement :as st]
   [er-cassandra.session :as session]
   [manifold.deferred :as d]
   [plumbing.core :refer [assoc-when]]
   [prpr.promise :as pr :refer [ddo]]
   [prpr.stream :as stream])
  (:import
   er_cassandra.session.Session))

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
         select-stmt ((if prepare?
                        st/prepare-select-statement
                        st/select-statement)
                      table
                      key
                      record-or-key-value
                      select-opts)
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
   (ddo [:let [select-opts (select-keys opts [:columns
                                              :limit
                                              :where
                                              :allow-filtering])
               select-stmt ((if prepare?
                              st/prepare-select-statement
                              st/select-statement)
                            table
                            select-opts)
               ps-values (when prepare?
                           (st/prepare-select-values
                            table
                            select-opts))]
         r-s (session/execute-buffered
              session
              select-stmt
              (-> opts
                  (dissoc :columns :limit)
                  (assoc-when :values ps-values)))]
     (return
      (r.ss/maybe-sorted-stream
       opts
       r-s))))

  ([^Session session table key record-or-key-value]
   (select-buffered session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {prepare? :prepare? :as opts}]
   (ddo [:let [select-opts (select-keys opts [:columns :where :only-if :order-by :limit])
               select-stmt ((if prepare?
                              st/prepare-select-statement
                              st/select-statement)
                            table
                            key
                            record-or-key-value
                            select-opts)
               ps-values (when prepare?
                           (st/prepare-select-values
                            table
                            key
                            record-or-key-value
                            select-opts))]
         r-s (session/execute-buffered
              session
              select-stmt
              (-> opts
                  (dissoc :columns :where :only-if :order-by :limit)
                  (assoc-when :values ps-values)))]
     (return
      (r.ss/maybe-sorted-stream
       opts
       r-s)))))

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

  ([^Session session
    table
    record
    {prepare? :prepare?
     :as opts}]
   (ddo [:let [insert-opts (select-keys opts [:if-not-exists :using])
               insert-stmt ((if prepare?
                              st/prepare-insert-statement
                              st/insert-statement)
                            table
                            record
                            insert-opts)
               insert-values (when prepare?
                               (st/prepare-insert-values
                                table
                                record
                                insert-opts))]
         [r _] (session/execute
                session
                insert-stmt
                (-> opts
                    (dissoc :if-not-exists :using)
                    (assoc-when :values insert-values)))]
     (return r))))

(defn insert-buffered
  "Returns a deferred of a stream of insert results."
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
        (stream/map-concurrently
         (or buffer-size 25)
         (fn [r]
           (insert session
                   table
                   r
                   (dissoc opts :buffer-size))))
        (return deferred-context))))

(defn update
  "update a single record"

  ([^Session session table key record]
   (update session table key record {}))

  ([^Session session
    table
    key
    record
    {prepare? :prepare?
     :as opts}]
   (ddo [:let [update-opts (select-keys opts [:only-if :if-exists :using :set-columns])
               stmt ((if prepare?
                       st/prepare-update-statement
                       st/update-statement)
                     table
                     key
                     record
                     update-opts)
               update-values (when prepare?
                               (st/prepare-update-values
                                table
                                key
                                record
                                update-opts))]
         [resp _] (session/execute
                   session
                   stmt
                   (-> opts
                       (dissoc :only-if :if-exists :using :set-columns)
                       (assoc-when :values update-values)))]
     (return resp))))

(defn update-buffered
  "update a stream of records"
  ([^Session session table key record-stream]
   (update-buffered session
                    table
                    key
                    record-stream
                    {:buffer-size 25}))

  ([^Session session
    table
    key
    record-stream
    {:keys [buffer-size] :as opts}]
   (->> record-stream
        (stream/map-concurrently
         (or buffer-size 25)
         (fn [r]
           (update session
                   table
                   key
                   r
                   (dissoc opts :buffer-size))))
        (return deferred-context))))

(defn delete
  "delete a record"

  ([^Session session table key record-or-key-value]
   (delete session table key record-or-key-value {}))

  ([^Session session
    table
    key
    record-or-key-value
    {prepare? :prepare?
     :as opts}]
   (ddo [:let [delete-opts (select-keys opts [:only-if :if-exists :using :where])
               delete-stmt ((if prepare?
                              st/prepare-delete-statement
                              st/delete-statement)
                            table
                            key
                            record-or-key-value
                            delete-opts)
               delete-values (when prepare?
                               (st/prepare-delete-values
                                table
                                key
                                record-or-key-value
                                delete-opts))]
         [resp _] (session/execute
                   session
                   delete-stmt
                   (-> opts
                       (dissoc :only-if :if-exists :using :where)
                       (assoc-when :values delete-values)))]
     (return
      resp))))

(defn delete-buffered
  "delete a stream of records"
  ([^Session session table key record-or-key-value-stream]
   (delete-buffered session
                    table
                    key
                    record-or-key-value-stream
                    {:buffer-size 25}))

  ([^Session session
    table
    key
    record-or-key-value-stream
    {:keys [buffer-size] :as opts}]
   (->> record-or-key-value-stream
        (stream/map-concurrently
         (or buffer-size 25)
         (fn [r-or-kv]
           (delete session
                   table
                   key
                   r-or-kv
                   (dissoc opts :buffer-size))))
        (return deferred-context))))
