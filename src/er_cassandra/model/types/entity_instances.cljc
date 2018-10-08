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

(defn entity-class-schema
  [^Entity entity]
  (when-let [class-name (t/entity-class-name entity)]
    {(s/optional-key entity-instance-class-name-key) (s/eq class-name)}))

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
