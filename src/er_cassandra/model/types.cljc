(ns er-cassandra.model.types
  (:require
   [clojure.set :as set]
   [schema.core :as s]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :as k]
   [er-cassandra.model.callbacks.schema :refer [CallbacksSchema]]
   [taoensso.timbre :refer [info warn]]))

(s/defschema NamespacedKeyword (s/constrained s/Keyword #(some? (namespace %)) 'keyword-namespace))

(s/defschema RecordSchema {s/Keyword s/Any})
(s/defschema MaybeRecordSchema (s/maybe RecordSchema))
(s/defschema ChangeSchema [(s/one MaybeRecordSchema :old-record)
                           (s/one MaybeRecordSchema :new-record)])

;; a primary key for a cassandra table
;; the first entry may itself be a vector, representing
;; a compound partition key
(s/defschema PrimaryKeySchema
  (s/conditional

   (fn [k] (-> k first sequential?))
   [(s/one
     [(s/one s/Keyword :partition-key-component) s/Keyword]
     :compound-parition-key)
    s/Keyword]

   :else
   [(s/one s/Keyword :key-component) s/Keyword]))

(s/defschema KeyValueSchema
  [(s/one s/Any :key-component) s/Any])

(s/defschema ForeignKeySchema
  [(s/one s/Keyword :foreign-key-component) s/Keyword])

;; a secondary key on a cassandra table has only
;; a single column
(s/defschema SecondaryKeySchema
  s/Keyword)

(s/defschema DeriveDenormFnSchema
  [(s/one #{s/Keyword} :source-cols)
   (s/one (s/pred fn?) :derivation-fn)])

(s/defschema DenormalizeColumnSchema
  (s/conditional
   vector? DeriveDenormFnSchema
   :else s/Keyword))

;; a map of target fields from source fields-or-fns
(s/defschema DenormalizeSchema
  {s/Keyword DenormalizeColumnSchema})

;; a Relationship allows fields from a parent Entity or
;; one of its index tables to be
;; denormalized to records of a child Entity. 1-1 and 1-many
;; relationships are supported and large child sets are also
;; supported
(s/defschema DenormalizationRelationshipSchema
  {;; namespace qualified keyword referencing the target Entity var.
   ;; will be dynamically deref'd
   :target (s/conditional
            keyword? s/Keyword
            :else {s/Keyword s/Any})
   ;; the fields to be denormalized
   :denormalize DenormalizeSchema
   ;; what to do with target records if a source record
   ;; is deleted
   :cascade (s/enum :none :null :delete)

   ;; the foreign key which must have the corresponding components
   ;; in the same order as the parent primary key
   :foreign-key ForeignKeySchema})

;; basic table schema shared by primary, secondary
;; unique-key and lookup tables
(s/defschema BaseTableSchema
  {:name s/Keyword
   :key PrimaryKeySchema})

;; the :key is the primary-key of the table, which may
;; have a compound parition key [[pk1 pk2] ck1 ck2]
(s/defschema PrimaryTableSchema
  (merge BaseTableSchema
         {:type (s/eq :primary)}))

;; some of the (non-partition) columns in a lookup-table
;; key may be collections... these will be expanded to the
;; the list of values formed by the cartesian product of all
;; the collection columns in the key. the
;; :collections metadata describes the type of each
;; collection column
(s/defschema CollectionKeysSchema
  {(s/optional-key :collections) {s/Keyword
                                  (s/enum :list :set :map)}})

(s/defschema MaterializedViewSchema
  {(s/optional-key :view?) s/Bool})

;; secondary tables contain all columns from the primary
;; table, with a different primary key.
;; secondary and lookup tables may be materialized views,
;; which will be used for query but won't be upserted to
(s/defschema SecondaryTableSchema
  (merge BaseTableSchema
         MaterializedViewSchema
         {:type (s/eq :secondary)}))

;; unique-key tables are lookup tables with a unique
;; constraint on the key, enforced with an LWT.
;; additional columns can be copied to the table with
;; :with-columns and the record generation can be
;; completely customised with a :generator-fn
;; which will be called with
;; (generator-fn cassandra model table old-record new-record) and
;; should return a list of lookup records or a Deferred thereof
(def UniqueKeyTableSchema
  (merge BaseTableSchema
         CollectionKeysSchema
         {:type (s/eq :uniquekey)
          (s/optional-key :with-columns) (s/conditional
                                          keyword? (s/eq :all)
                                          :else [s/Keyword])
          (s/optional-key :generator-fn) (s/pred fn? "generator-fn")}))

;; lookup tables contain columns from the uberkey and
;; a lookup key, plus any additional with-columns
;; a generator-fn may be supplied which will be called with
;; (generator-fn cassandra model table old-record new-record) and
;; should return a list of lookup records or a Deferred thereof.
;; if no generator-fn is supplied then a default is used
(s/defschema LookupTableSchema
  (s/conditional
   :generator-fn (merge UniqueKeyTableSchema
                        {:type (s/eq :lookup)})
   :else (merge UniqueKeyTableSchema
                MaterializedViewSchema
                {:type (s/eq :lookup)})))

(s/defschema IndexTableSchema
  (s/conditional
   #(= (:type %) :uniquekey) UniqueKeyTableSchema
   #(= (:type %) :lookup) LookupTableSchema))

(s/defschema TableSchema
  (s/conditional
   #(= (:type %) :primary) PrimaryTableSchema
   #(= (:type %) :secondary) SecondaryTableSchema
   #(= (:type %) :uniquekey) UniqueKeyTableSchema
   #(= (:type %) :lookup) LookupTableSchema))

(s/defschema EntitySchema
  {:class-name (s/maybe NamespacedKeyword)
   :primary-table PrimaryTableSchema
   (s/optional-key :unique-key-tables) [UniqueKeyTableSchema]
   (s/optional-key :secondary-tables) [SecondaryTableSchema]
   (s/optional-key :lookup-tables) [LookupTableSchema]
   (s/optional-key :callbacks) CallbacksSchema
   (s/optional-key :denorm-targets) {s/Keyword DenormalizationRelationshipSchema}
   (s/optional-key :denorm-sources) {s/Keyword s/Keyword}})


(s/defrecord Entity
    [class-name :- (s/maybe NamespacedKeyword)
     primary-table :- PrimaryTableSchema
     unique-key-tables :- [UniqueKeyTableSchema]
     secondary-tables :- [SecondaryTableSchema]
     lookup-tables :- [LookupTableSchema]
     callbacks :- CallbacksSchema
     denorm-targets :- {s/Keyword DenormalizationRelationshipSchema}
     denorm-sources :- {s/Keyword s/Keyword}])

(defn ^:private conform-table
  [table-type table]
  (assoc table
         :type table-type
         :key (v/coerce (:key table))))

(defn ^:private conform-tables
  [entity-schema [table-type table-seq-key]]
  (assoc entity-schema
         table-seq-key
         (mapv (partial conform-table table-type)
               (get entity-schema table-seq-key))))

(defn ^:private conform-all-tables
  "ensure all keys are given as sequences of components"
  [entity-schema]
  (reduce conform-tables
          (assoc entity-schema
                 :primary-table
                 (conform-table :primary (:primary-table entity-schema)))
          [[:uniquekey :unique-key-tables]
           [:secondary :secondary-tables]
           [:lookup :lookup-tables]]))

(s/defn ^:always-validate create-entity :- Entity
  "create an entity record from a spec"
  [entity-spec]
  (let [spec (conform-all-tables entity-spec)
        spec (merge {:class-name nil
                     :unique-key-tables []
                     :secondary-tables []
                     :lookup-tables []
                     :callbacks {}
                     :denorm-targets {}
                     :denorm-sources {}}
                    spec)]
    (s/validate EntitySchema spec)
    (strict-map->Entity spec)))

#?(:clj
   (defmacro defentity
     [var-name entity-spec]
     (let [class-name (keyword (name (ns-name *ns*)) (name var-name))]
       `(def ~var-name
          (create-entity
           (assoc
            ~entity-spec
            :class-name ~class-name))))))

(defn satisfies-entity?
  "return true if the record has at least all columns of
   the entity uber-key"
  [entity record]
  (if (map? record)
    (let [r-cols (-> record keys set)
          uk-cols (-> entity :primary-table :key flatten set)]
      (empty? (set/difference uk-cols r-cols)))
    false))

(defn satisfies-primary-key?
  "return true if key is the same as the full primary-key"
  [primary-key key]
  (assert (sequential? primary-key))
  (assert (sequential? key))
  (= (flatten primary-key) (flatten key)))

(defn satisfies-partition-key?
  "return true if key is the same as the partition-key"
  [primary-key key]
  (assert (sequential? primary-key))
  (assert (sequential? key))
  (= (k/partition-key primary-key) key))

(defn satisfies-cluster-key?
  "return true if key matches the full partition-key plus
   some prefix of the cluster-key"
  [primary-key key]
  (assert (sequential? primary-key))
  (assert (sequential? key))
  (let [pkpk (k/partition-key primary-key)
        pkck (k/cluster-key primary-key)
        pkck-c (count pkck)

        kpk (take (count pkpk) (flatten key))
        kck (not-empty (drop (count pkpk) (flatten key)))
        kck-c (count kck)

        spk? (satisfies-partition-key? primary-key kpk)]
    (cond
      (> kck-c pkck-c)
      false

      (= kck-c pkck-c)
      (and spk?
           (= kck pkck))

      :else
      (do
        (and spk?
             (= kck
                (not-empty (take kck-c pkck))))))))

(defn- is-table-name
  [tables table]
  (some (fn [t] (when (= table (:name t)) t))
        tables))

(defn is-primary-table
  [^Entity entity table]
  (is-table-name [(:primary-table entity)] table))

(defn is-secondary-table
  [^Entity entity table]
  (is-table-name (:secondary-tables entity) table))

(defn is-unique-key-table
  [^Entity entity table]
  (is-table-name (:unique-key-tables entity) table))

(defn is-lookup-table
  [^Entity entity table]
  (is-table-name (:lookup-tables entity) table))

(defn entity-class-name
  [^Entity entity]
  (:class-name entity))

(defn uber-key
  [^Entity entity]
  (get-in entity [:primary-table :key]))

(defn unique-key-tables
  [^Entity entity]
  (->> entity
       :unique-key-tables))

(defn mutable-secondary-tables
  [^Entity entity]
  (->> entity
       :secondary-tables
       (filterv (comp not :view?))))

(defn mutable-lookup-tables
  [^Entity entity]
  (->> entity
       :lookup-tables
       (filterv (comp not :view?))))

(defn all-key-cols
  "a list of all cols used in keys across all tables for the entity"
  [^Entity entity]
  (distinct
   (flatten
    (concat (uber-key entity)
            (mapcat :key (:unique-key-tables entity))
            (mapcat :key (:secondary-tables entity))
            (mapcat :key (:lookup-tables entity))))))

(defn all-maintained-foreign-key-cols
  "a list of all cols used in foreign keys maintained by this lib (rather than c* MVs)"
  [^Entity entity]
  (distinct
   (flatten
    (concat
     (->> entity :unique-key-tables (mapcat :key))
     (->> entity :secondary-tables (filter (complement :view?)) (mapcat :key))
     (->> entity :lookup-tables (filter (complement :view?)) (mapcat :key))))))

(defn all-entity-tables
  "returns a list of all tables from an entity"
  [^Entity entity]
  (concat
   [(:primary-table entity)]
   (:secondary-tables entity)
   (:lookup-tables entity)
   (:unique-key-tables entity)))

(defn contains-key-cols-for-table?
  "true if the record has an entry for
   every column of the table's key"
  [^Entity entity record table]
  (let [kcols (flatten (:key table))]
    (every? #(contains? record %) kcols)))

(defn extract-uber-key-value
  [^Entity entity record]
  (let [kv (k/extract-key-value
            (get-in entity [:primary-table :key])
            record)]
    (when (nil? kv)
      (throw (ex-info "nil uberkey" {:entity entity :record record
                                     :cause ::nil-uberkey})))
    kv))

(defn extract-uber-key-equality-clause
  [^Entity entity record]
  (k/extract-key-equality-clause
   (get-in entity [:primary-table :key])
   record))
