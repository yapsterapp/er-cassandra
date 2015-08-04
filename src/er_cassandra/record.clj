(ns er-cassandra.record
  (:require
   [plumbing.core :refer :all]
   [qbits.alia.manifold :as aliam]
   [qbits.hayt :as h]
   [clj-uuid :as uuid]))

(defn make-sequential
  [v]
  (cond (nil? v) v
        (sequential? v) v
        :else [v]))

(defn extract-key-clause
  ([key record] (extract-key-clause key record {}))
  ([key record {:keys [key-value]}]
   (let [key (make-sequential key)
         use-key-value (or (make-sequential key-value)
                           (repeat nil))
         use-key-value (map (fn [k dv]
                              (let [kv (or dv (get record k))]
                                (when-not kv
                                  (throw (ex-info "no key value"
                                                  {:key key
                                                   :record record
                                                   :key-value key-value})))
                                kv))
                            key
                            use-key-value)]
     (mapv (fn [k v]
             [:= k v])
           key
           use-key-value))))

(defn select
  ([session table key record-or-key-value]
   (select session table key record-or-key-value {}))
  ([session table key record-or-key-value {:keys [key-value] :as opts}]
   (let [key-value (or key-value
                       (when-not (map? record-or-key-value)
                         record-or-key-value))
         record (when (map? record-or-key-value)
                  record-or-key-value)
         key-clause (extract-key-clause key record {:key-value key-value})]
     (aliam/execute
      session
      (h/->raw
       (h/select table
                 (h/where key-clause)))))))

(defn upsert
  "upsert a single record"
  ([session table key record]
   (upsert session table key record {}))
  ([session table key record {:keys [key-value] :as opts}]
   (let [key-clause (extract-key-clause key record opts)
         set-cols (apply dissoc record (make-sequential key))]
     (aliam/execute
      session
      (h/->raw
       (h/update table
                 (h/set-columns set-cols)
                 (h/where key-clause)))))))

(defn delete
  ([session table key record-or-key-value]
   (delete session table key record-or-key-value {}))
  ([session table key record-or-key-value {:keys [key-value] :as opts}]
   (let [key-value (or key-value
                       (when-not (map? record-or-key-value)
                         record-or-key-value))
         record (when (map? record-or-key-value)
                  record-or-key-value)
         key-clause (extract-key-clause key record {:key-value key-value})]
     (aliam/execute
      session
      (h/->raw
       (h/delete table
                 (h/where key-clause)))))))
