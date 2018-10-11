(ns er-cassandra.model.types.entity-instances
  (:require
   [er-cassandra.model.types :as t
    #?@(:cljs [:refer [Entity]])]
   [schema.core :as s #?@(:cljs [:include-macros true])])
  #?(:cljs
     (:require-macros
      [er-cassandra.model.types.entity-instances
       :refer [record-schema
               defrecordschema]]))
  #?(:clj
     (:import
      [er_cassandra.model.types Entity])))

(def entity-instance-class-name-key
  :er-cassandra.model/entity)

(def extra-keys-schema
  #?(:clj nil
     :cljs {s/Keyword s/Any}))

(def entity-class-name
  t/entity-class-name)

(defn entity-class-schema
  [^Entity entity]
  (when-let [class-name (t/entity-class-name entity)]
    {(s/optional-key entity-instance-class-name-key) (s/eq class-name)}))

;; NOTE mccraig-20181011 this looks weird - the reduce
;; doesn't seem to do anything
(defn with-optional-entity-class
  [sch ^Entity entity]
  (if (map? sch)
    (merge
     (dissoc sch entity-instance-class-name-key)
     (reduce
      (fn [rs [k v]]
        (assoc
         rs
         (if (s/optional-key? k)
           k
           (s/optional-key k))
         v))
      {}
      (entity-class-schema entity)))
    sch))

#?(:clj
   (defmacro record-schema
     [^Entity entity record-schema]
     `(let [base-schema# ~record-schema
            is-map-schema?# (map? base-schema#)
            extra-keys# (when is-map-schema?#
                          (s/find-extra-keys-schema
                           base-schema#))
            has-extra-keys?# (some? extra-keys#)]
        (if-not is-map-schema?#
          base-schema#
          (merge
           base-schema#
           (entity-class-schema ~entity)
           (when-not has-extra-keys?#
             extra-keys-schema))))))

#?(:clj
   (defmacro defrecordschema
     [name ^Entity entity record-schema]
     `(s/defschema ~name
        (record-schema
         ~entity
         ~record-schema))))

(defn decorate-with-entity-class
  [^Entity entity record]
  (let [class-name (t/entity-class-name entity)]
    (assoc record entity-instance-class-name-key class-name)))
