(ns er-cassandra.model.select
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [cats.labs.manifold :refer [deferred-context]]
            [er-cassandra.model.types :as t]
            [er-cassandra.model.model-session :as ms]
            [er-cassandra.model.util
             :refer [combine-responses
                     combine-seq-responses]])
  (:import
   [er_cassandra.model.types Entity]
   [er_cassandra.model.model_session ModelSession]))

(defn select-buffered
  "select-buffered is now the fundamental select operation - this enables
   running callbacks on the results stream sensibly"
  ([^ModelSession session ^Entity entity]
   (select-buffered session entity {}))

  ([^ModelSession session ^Entity entity opts]
   (with-context deferred-context
     (mlet [strm (ms/-select-buffered session entity opts)]
       (->> strm
            (s/map (fn [mi]
                     (if (ms/entity-instance? mi)
                       (t/run-callbacks
                        entity
                        :after-load
                        mi
                        opts)
                       mi)))
            s/realize-each
            return))))

  ;; can't provide an arity which auto-selects the uber-key, because it's
  ;; already used for a full-table select

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (select-buffered session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (mlet [strm (ms/-select-buffered session entity key record-or-key-value opts)]
       (->> strm
            (s/map (fn [mi]
                     (if (ms/entity-instance? mi)
                       (t/run-callbacks
                        entity
                        :after-load
                        mi
                        opts)
                       mi)))
            s/realize-each
            return)))))

(defn select
  ([^ModelSession session ^Entity entity record-or-key-value]
   (select session entity (t/uber-key entity) record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (select session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (mlet [select-s (select-buffered session entity key record-or-key-value opts)]
       (s/reduce conj [] select-s)))))

(defn select-one
  "select a single record, using an index table if necessary"

  ([^ModelSession session ^Entity entity record-or-key-value]
   (select-one session entity (t/uber-key entity) record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (select-one session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (mlet [select-s (select-buffered
                      session
                      entity
                      key
                      record-or-key-value
                      (merge opts {:limit 1}))]
       (s/take! select-s)))))

(defn select-one-instance
  "select a single record, unless the record is already a record retrieved
   from the db, in which case return it"
  ([^ModelSession session ^Entity entity record-or-key-value]
   (select-one-instance session entity (t/uber-key entity) record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (select-one-instance session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (if (ms/entity-instance? record-or-key-value)
       (return record-or-key-value)
       (select-one session entity key record-or-key-value opts)))))

(defn ensure-one
  "select a single record erroring the response if there is no record"
  ([^ModelSession session ^Entity entity record-or-key-value]
   (ensure-one session entity (t/uber-key entity) record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (ensure-one session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (mlet [r (select-one session
                          entity
                          key
                          record-or-key-value
                          opts)]
       (if r
         (return r)
         (d/error-deferred (ex-info
                            "no record"
                            {:reason [:fail
                                      {:entity entity
                                       :key key
                                       :record-or-key-value record-or-key-value}
                                      :no-matching-record]})))))))

(defn ensure-one-instance
  "select-one-instance but errors if there is no record"
  ([^ModelSession session ^Entity entity record-or-key-value]
   (ensure-one-instance session entity (t/uber-key entity) record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value]
   (ensure-one-instance session entity key record-or-key-value {}))

  ([^ModelSession session ^Entity entity key record-or-key-value opts]
   (with-context deferred-context
     (if (ms/entity-instance? record-or-key-value)
         (return record-or-key-value)
         (ensure-one session entity key record-or-key-value opts)))))

(defn select-many-buffered
  "issue one select-one query for each record-or-key-value and combine
   the responses"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many-buffered session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many-buffered session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (->> (or record-or-key-values '())
        (s/map (fn [record-or-key-value]
                 (select-one session entity key record-or-key-value opts)))
        (s/realize-each)
        (s/filter some?)
        return)))

(defn select-many
  "issue one select-one query for each record-or-key-value and combine
   the responses"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (with-context deferred-context
     (mlet [sm-s (select-many-buffered
                  session
                  entity
                  key
                  record-or-key-values
                  opts)]
       (s/reduce conj [] sm-s)))))

(defn select-many-instances-buffered
  "select-many records, unless the record-or-key-values were already
  retrives from the db, in which case return them directly (but still
  select any which were not already retrieved from the db)"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many-instances-buffered session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many-instances-buffered session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (with-context deferred-context
     (->> record-or-key-values
          (s/map (fn [record-or-key-value]
                   (select-one-instance session entity key record-or-key-value opts)))
          (s/realize-each)
          (s/filter some?)
          return))))

(defn select-many-instances
  "select-many records, unless the record-or-key-values were already
  retrives from the db, in which case return them directly (but still
  select any which were not already retrieved from the db)"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many-instances session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many-instances session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (with-context deferred-context
     (mlet [smib-s (select-many-instances-buffered
                    session
                    entity
                    key
                    record-or-key-values
                    opts)]
       (s/reduce conj [] smib-s)))))

(defn select-many-cat-buffered
  "issue one select query for each record-or-key-value and concatenate
   the responses"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many-cat-buffered session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many-cat-buffered session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (with-context deferred-context
     (->> (or record-or-key-values '())
          (s/map (fn [record-or-key-value]
                   (select-buffered session entity key record-or-key-value opts)))
          (s/realize-each)
          (s/concat)
          (s/filter some?)
          return))))

(defn select-many-cat
  "issue one select query for each record-or-key-value and concatenates
   the responses"
  ([^ModelSession session ^Entity entity record-or-key-values]
   (select-many-cat session entity (t/uber-key entity) record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values]
   (select-many-cat session entity key record-or-key-values {}))

  ([^ModelSession session ^Entity entity key record-or-key-values opts]
   (with-context deferred-context
     (mlet [smb-s (select-many-cat-buffered
                   session
                   entity
                   key
                   record-or-key-values
                   opts)]
       (s/reduce conj [] smb-s)))))
