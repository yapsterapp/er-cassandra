(ns er-cassandra.model.types
  (:require
   [cats.core :refer [mlet return]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [schema.core :as s]
   [clj-time.core :as t]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :as k]))

(s/defschema RecordSchema {s/Keyword s/Any})
(s/defschema MaybeRecordSchema (s/maybe RecordSchema))

(s/defschema CallbackFnSchema
  (s/make-fn-schema s/Any [[{s/Keyword s/Any}]]))

(s/defschema CallbacksSchema
  {(s/optional-key :after-load) [CallbackFnSchema]
   (s/optional-key :before-save) [CallbackFnSchema]})

;; a primary key for a cassandra table
;; the first entry may itself be a vector, representing
;; a compound partition key
(s/defschema KeySchema
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

;; a secondary key on a cassandra table has only
;; a single column
(s/defschema SecondaryKeySchema
  s/Keyword)

;; just map parent fidles to child fields for now
;; :foreign-key and :parent-entity-key will be
;; included by default
(s/defschema DenormalizeSchema
  {s/Keyword s/Keyword})

;; a Relationship allows fields from a parent Entity or
;; one of its index tables to be
;; denormalized to records of a child Entity. 1-1 and 1-many
;; relationships are supported and large child sets are also
;; supported
(s/defschema RelationshipSchema
  {;; target Entity var - will be dynamically deref'd
   :target s/Symbol
   ;; the foreign key which must have the corresponding components
   ;; in the same order as the parent primary key
   :foreign-key KeySchema
   ;; the fields to be denormalized
   :denormalize DenormalizeSchema})

;; basic table schema shared by primary, secondary
;; unique-key and lookup tables
(s/defschema BaseTableSchema
  {:name s/Keyword
   :key KeySchema
   (s/optional-key :relationships) [RelationshipSchema]})

;; the :key is the primary-key of the table, which may
;; have a compound parition key [[pk1 pk2] ck1 ck2]
;; the :entity-key is an optional unique identifier for which
;; a value taken from the primary record will be used to
;; delete with a secondary index from seocondary and lookup tables
;; TODO use the :entity-key to delete stale secondary and lookup records
(s/defschema PrimaryTableSchema
  (merge BaseTableSchema
         {(s/optional-key :entity-key) SecondaryKeySchema}
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

(s/defschema EntityKeySchema
  {(s/optional-key :has-entity-key?) s/Bool})

(s/defschema MaterializedViewSchema
  {(s/optional-key :view?) s/Bool})

;; unique-key tables are lookup tables with a unique
;; constraint on the key, enforced with an LWT
(s/defschema UniqueKeyTableSchema
  (merge BaseTableSchema
         CollectionKeysSchema
         EntityKeySchema
         {:type (s/eq :uniquekey)}))

;; secondary tables contain all columns from the primary
;; table, with a different primary key.
;; secondary and lookup tables may be materialized views,
;; which will be used for query but won't be upserted to
(s/defschema SecondaryTableSchema
  (merge BaseTableSchema
         EntityKeySchema
         MaterializedViewSchema
         {:type (s/eq :secondary)}))

;; lookup tables contain columns from the uberkey and
;; a lookup key, plus any additional with-columns
(s/defschema LookupTableSchema
  (merge SecondaryTableSchema
         CollectionKeysSchema
         MaterializedViewSchema
         {(s/optional-key :with-columns) [s/Keyword]}
         {:type (s/eq :lookup)}))

(s/defschema TableSchema
  (s/conditional
   #(= (:type %) :primary) PrimaryTableSchema
   #(= (:type %) :secondary) SecondaryTableSchema
   #(= (:type %) :uniquekey) UniqueKeyTableSchema
   #(= (:type %) :lookup) LookupTableSchema))

(s/defschema EntitySchema
  {:primary-table PrimaryTableSchema
   (s/optional-key :unique-key-tables) [UniqueKeyTableSchema]
   (s/optional-key :secondary-tables) [SecondaryTableSchema]
   (s/optional-key :lookup-key-tables) [LookupTableSchema]
   (s/optional-key :callbacks) CallbacksSchema
   (s/optional-key :versioned?) s/Bool})


(s/defrecord Entity
    [primary-table :- PrimaryTableSchema
     unique-key-tables :- [UniqueKeyTableSchema]
     secondary-tables :- [SecondaryTableSchema]
     lookup-key-tables :- [LookupTableSchema]
     callbacks :- CallbacksSchema
     versioned? :- s/Bool])

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
           [:lookup :lookup-key-tables]]))

(s/defn ^:always-validate create-entity :- Entity
  "create an entity record from a spec"
  [entity-spec]
  (let [spec (conform-all-tables entity-spec)]
    (s/validate EntitySchema spec)
    (strict-map->Entity
     (merge {:unique-key-tables []
             :secondary-tables []
             :lookup-key-tables []
             :callbacks {}
             :versioned? false}
            spec))))

(defmacro defentity
  [name entity-spec]
  `(def ~name (create-entity ~entity-spec)))

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

(defn is-lookup-key-table
  [^Entity entity table]
  (is-table-name (:lookup-key-tables entity) table))

(defn uber-key
  [^Entity entity]
  (get-in entity [:primary-table :key]))

(defn mutable-secondary-tables
  [^Entity entity]
  (->> entity
       :secondary-tables
       (filterv (comp not :view?))))

(defn mutable-lookup-tables
  [^Entity entity]
  (->> entity
       :lookup-key-tables
       (filterv (comp not :view?))))

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

(defn run-callbacks
  ([^Entity entity callback-key records]
   (run-callbacks entity callback-key records {}))
  ([^Entity entity callback-key records opts]
   (let [callbacks (concat (get-in entity [:callbacks callback-key])
                           (get-in opts [callback-key]))]
     (try
       (reduce (fn [r callback]
                 (mapv callback r))
               records
               callbacks)
       (catch Exception ex
         (throw (ex-info (format "Failed to run callbacks '%s' on record of entity for '%s'"
                                 callback-key
                                 (get-in entity [:primary-table :name]))
                         {:callback-key callback-key
                          :entity entity}
                         ex)))))))

(defn run-callbacks-single
  ([^Entity entity callback-key record]
   (run-callbacks-single entity callback-key record {}))
  ([^Entity entity callback-key record opts]
   (let [callbacks (concat (get-in entity [:callbacks callback-key])
                           (get-in opts [callback-key]))]
     (reduce (fn [r callback]
               (callback r))
             record
             callbacks))))

(defn run-deferred-callbacks
  ([^Entity entity callback-key deferred-records]
   (run-deferred-callbacks entity callback-key deferred-records {}))
  ([^Entity entity callback-key deferred-records opts]
   (with-context deferred-context
     (mlet [records deferred-records]
       (return
        (run-callbacks entity callback-key records opts))))))

(defn run-deferred-callbacks-single
  ([^Entity entity callback-key deferred-record]
   (run-deferred-callbacks-single entity callback-key deferred-record {}))
  ([^Entity entity callback-key deferred-record opts]
   (with-context deferred-context
     (mlet [record deferred-record]
       (return
        (run-callbacks-single entity callback-key record opts))))))

(defn create-protect-columns-callback
  "create a callback which will remove cols from a record
   unless the confirm-col is set and non-nil. always
   removes confirm-col"
  [confirm-col & cols]
  (fn [r]
    (if (get r confirm-col)
      (dissoc r confirm-col)
      (apply dissoc r confirm-col cols))))

(defn create-updated-at-callback
  "create a callback which will add an :updated_at column
   if it's not already set"
  ([] (create-updated-at-callback :updated_at))
  ([updated-at-col]
   (fn [r] (assoc r updated-at-col (.toDate (t/now))))))

(defn create-select-view-callback
  "selects the given columns from a record"
  [cols]
  (fn [r]
    (select-keys r cols)))

(defn create-filter-view-callback
  "filers the given columns from a record"
  [cols]
  (fn [r]
    (apply dissoc r cols)))

(defn create-update-col-callback
  "a callback which updates a column with a function"
  [col f]
  (fn [r]
    (update r col f)))
