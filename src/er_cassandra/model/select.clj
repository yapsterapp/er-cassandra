(ns er-cassandra.model.select
  (:require [manifold.deferred :as d]
            [cats.core :refer [mlet return]]
            [cats.context :refer [with-context]]
            [cats.labs.manifold :refer [deferred-context]]
            [er-cassandra.model.types :as t]
            [er-cassandra.model.model-session :as ms]
            [er-cassandra.model.util
             :refer [combine-responses
                     combine-seq-responses]])
  (:import
   [er_cassandra.model.types Model]
   [er_cassandra.model.model_session ModelSession]))

(defn select
  ([^ModelSession session ^Model model key record-or-key-value]
   (select session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]
   (t/run-deferred-callbacks
    model
    :after-load
    (ms/-select session model key record-or-key-value opts)
    opts)))

(defn select-one
  "select a single record, using an index table if necessary"

  ([^ModelSession session ^Model model key record-or-key-value]
   (select-one session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]
   (with-context deferred-context
     (mlet [records (select session
                            model
                            key
                            record-or-key-value
                            (merge opts {:limit 1}))]
       (return (first records))))))

(defn select-one-instance
  "select a single record, unless the record is already a record retrieved
   from the db, in which case return it"
  ([^ModelSession session ^Model model key record-or-key-value]
   (select-one-if session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]
   (with-context deferred-context
     (if (ms/model-instance? record-or-key-value)
       (return record-or-key-value)
       (select-one session model key record-or-key-value opts)))))

(defn ensure-one
  "select a single record erroring the response if there is no record"
  ([^ModelSession session ^Model model key record-or-key-value]
   (ensure-one session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]
   (with-context deferred-context
     (mlet [records (select session
                            model
                            key
                            record-or-key-value
                            (merge opts {:limit 1}))]
       (if (empty? records)
         (d/error-deferred (ex-info
                            "no record"
                            {:reason [:fail
                                      {:model model
                                       :key key
                                       :record-or-key-value record-or-key-value}
                                      :no-matching-record]}))
         (return (first records)))))))

(defn ensure-one-instance
  "select-one-instance but errors if there is no record"
  ([^ModelSession session ^Model model key record-or-key-value]
   (ensure-one-instance session model key record-or-key-value {}))

  ([^ModelSession session ^Model model key record-or-key-value opts]
   (with-context deferred-context
     (if (ms/model-instance? record-or-key-value)
         (return record-or-key-value)
         (select-one-instance model key record-or-key-value opts)))))

(defn select-many
  "issue one select-one query for each record-or-key-value and combine
   the responses"

  ([^ModelSession session ^Model model key record-or-key-values]
   (select-many session model key record-or-key-values {}))
  ([^ModelSession session ^Model model key record-or-key-values opts]
   (->> record-or-key-values
        (map (fn [record-or-key-value]
               (select-one session model key record-or-key-value opts)))
        combine-responses)))

(defn select-many-instances
  "select-many records, unless the record-or-key-values were already
  retrives from the db, in which case return them directly (but still
  select any which were not already retrieved from the db)"
  ([^ModelSession session ^Model model key record-or-key-values]
   (select-many-instances session model key record-or-key-values {}))
  ([^ModelSession session ^Model model key record-or-key-values opts]
   (->> record-or-key-values
        (map (fn [record-or-key-value]
               (select-one-instance session model key record-or-key-value opts)))
        combine-responses)))

(defn select-many-cat
  "issue one select query for each record-or-key-value and concatenates
   the responses"
  ([^ModelSession session ^Model model key record-or-key-values]
   (select-many-cat session model key record-or-key-values {}))
  ([^ModelSession session ^Model model key record-or-key-values opts]
   (->> record-or-key-values
        (map (fn [record-or-key-value]
               (select session model key record-or-key-value)))
        combine-seq-responses)))
