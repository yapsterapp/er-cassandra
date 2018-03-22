(ns er-cassandra.record.sorted-stream
  (:require
   [er-cassandra.uuid :refer [-compare]]
   [prpr.stream.cross :as cross]
   [taoensso.timbre :refer [debug info warn]]))

(defn select-opts->key-fn
  "given select-opts, if there is an :order-by
   then create a key-fn which will extract the
   key from the response records, and can be
   used for a sorted-stream"
  [{order-by :order-by
    :as select-opts}]

  (when (not-empty order-by)
    (if (> (count order-by) 1)
      (apply
       juxt
       (for [[k dir] order-by]
         k))
      (get-in order-by [0 0]))))

(defn maybe-sorted-stream
  "if there is an :order-by in the select-opts
   then wrap the stream in a sorted-stream"
  [select-opts s]
  (info "maybe-sorted-stream" select-opts s)
  (if-let [kfn (select-opts->key-fn select-opts)]
    (cross/sorted-stream kfn s)
    s))
