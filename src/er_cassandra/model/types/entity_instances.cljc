(ns er-cassandra.model.types.entity-instances
  (:require
   [er-cassandra.model.types :as t
    #?@(:cljs [:refer [Entity]])]
   [schema.core :as s #?@(:cljs [:include-macros true])])
  #?(:clj
     (:import
      [er_cassandra.model.types Entity])))

(def entity-instance-class-name-key
  :er-cassandra.model/entity)

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

(defn decorate-with-entity-class
  [^Entity entity record]
  (let [class-name (t/entity-class-name entity)]
    (assoc record entity-instance-class-name-key class-name)))
