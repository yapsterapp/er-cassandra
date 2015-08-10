(ns er-cassandra.key)

(defn make-sequential
  [v]
  (cond (nil? v) v
        (sequential? v) v
        :else [v]))

(defn extract-key-value*
  ([key record-or-key-value {:keys [key-value]}]
   (let [key (make-sequential key)
         key-value (or (make-sequential key-value)
                       (if-not (map? record-or-key-value)
                         (make-sequential record-or-key-value)
                         (repeat (count key) nil)))
         record (when (map? record-or-key-value)
                  record-or-key-value)]

     (map (fn [k ev]
            (let [kv (or ev (get record k))]
              (when-not kv
                (throw (ex-info "missing key value component"
                                {:component k
                                 :key key
                                 :record record
                                 :key-value key-value})))
              kv))
          key
          key-value))))

(defn extract-key-value
  "extract a key value from some combination of explicit value
   and a record"

  ([key record-or-key-value]
   (extract-key-value key record-or-key-value {}))

  ([key record-or-key-value {:keys [collection] :as opts}]
   (extract-key-value* key record-or-key-value opts)))

(defn extract-key-equality-clause
  "returns a Hayt key equality clause for use in a (when...) form"

  ([key record-or-key-value]
   (extract-key-equality-clause key record-or-key-value {}))

  ([key record-or-key-value opts]
   (let [key (make-sequential key)
         kv (extract-key-value key record-or-key-value opts)]
     (mapv (fn [k v]
             [:= k v])
           key
           kv))))

(defn extract-collection-key-components
  [coll record-or-key-value]
  (cond
    (map? coll) (keys coll)
    (sequential? coll) coll
    (set? coll) coll
    :else (throw (ex-info
                  "not a supported key collection"
                  {:coll coll
                   :record-or-key-value record-or-key-value}))))

(defn extract-key-value-collection
  ([key record-or-key-value]
   (extract-key-value-collection key record-or-key-value {}))

  ([key record-or-key-value opts]
   (let [kv (extract-key-value* key record-or-key-value opts)]
     (let [pre (into [] (take (dec (count kv)) kv))
           coll (last kv)
           lkvs (extract-collection-key-components coll opts)]
       (->> lkvs
            (map (fn [lkv] (conj pre lkv )))
            set)))))
