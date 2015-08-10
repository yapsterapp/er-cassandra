(ns er-cassandra.model.delete
  (:require [manifold.deferred :as d]
            [qbits.alia :as alia]
            [qbits.alia.manifold :as aliam]
            [qbits.hayt :as h]
            [er-cassandra.key :as k]
            [er-cassandra.record :as r]
            [er-cassandra.model.types]
            [er-cassandra.model.select :refer [select-one]])
  (:import [er_cassandra.model.types Model]))

(defn delete
  "delete a single instance, removing primary, secondary and lookup records "

  ([session ^Model model key record-or-key-value]
   (delete session model key record-or-key-value {}))

  ([session ^Model model key record-or-key-value opts]
   ;; delete from primary-table where uber-key=X
   ;; delete from each secondary table where uber-key=X
   ;; delete from each lookup table where uber-key=X
   ))
