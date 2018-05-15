(ns er-cassandra.model.types
  (:require
   [cats.core :refer [mlet return >>=]]
   [cats.context :refer [with-context]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.set :as set]
   [schema.core :as s]
   [clj-time.core :as t]
   [er-cassandra.util.vector :as v]
   [er-cassandra.key :as k]))

(s/defschema RecordSchema {s/Keyword s/Any})
(s/defschema MaybeRecordSchema (s/maybe RecordSchema))
(s/defschema ChangeSchema [(s/one MaybeRecordSchema :old-record)
                           (s/one MaybeRecordSchema :new-record)])

(s/defschema CallbackFnSchema
  (s/make-fn-schema s/Any [[{s/Keyword s/Any}]]))

(defprotocol ICallback
  (-deserialize [_ session entity record opts])
  (-after-load [_ session entity record opts]
    "an after-load callback on a record of an entity, returning
     updated-record or Deferred<updated-record>")
  (-before-save [_ entity old-record record opts]
    "a before-save callback on a record of an entity, returning
     updated-record or Deferred<updated-record>. -before-save doesn't
     receive the session because it shouldn't do any persistence ops -
     -before-save is used internally during upsert")
  (-serialize [_ entity old-record record opts])
  (-after-save [_ session entity old-record record opts]
    "an after-save callback on a record of an entity. responses
     are ignored")
  (-before-delete [_ entity record opts]
    "a before-delete callback on a record of an entity. responses
     are ignored, but an error will prevent the delete. -before-delete
     doesn't receive the session because it shouldn't do any persistence ops")
  (-after-delete [_ session entity record opts]
    "an after-delete callback on a record of an entity. responses
     are ignored"))

(s/defschema CallbackSchema
  (s/conditional
   fn? CallbackFnSchema
   #(satisfies? ICallback %) (s/protocol ICallback)))

(s/defschema CallbacksSchema
  {(s/optional-key :deserialize) [CallbackSchema]
   (s/optional-key :after-load) [CallbackSchema]
   (s/optional-key :before-save) [CallbackSchema]
   (s/optional-key :serialize) [CallbackSchema]
   (s/optional-key :after-save) [CallbackSchema]
   (s/optional-key :before-delete) [CallbackSchema]
   (s/optional-key :after-delete) [CallbackSchema]})

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

(s/defschema KeywordOrFnSchema
  (s/conditional
   fn? (s/pred fn?)
   :else s/Keyword))

;; a map of target fields from source fields-or-fns
(s/defschema DenormalizeSchema
  {s/Keyword KeywordOrFnSchema})

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
  {:primary-table PrimaryTableSchema
   (s/optional-key :unique-key-tables) [UniqueKeyTableSchema]
   (s/optional-key :secondary-tables) [SecondaryTableSchema]
   (s/optional-key :lookup-tables) [LookupTableSchema]
   (s/optional-key :callbacks) CallbacksSchema
   (s/optional-key :denorm-targets) {s/Keyword DenormalizationRelationshipSchema}
   (s/optional-key :denorm-sources) {s/Keyword s/Keyword}})


(s/defrecord Entity
    [primary-table :- PrimaryTableSchema
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
        spec (merge {:unique-key-tables []
                     :secondary-tables []
                     :lookup-tables []
                     :callbacks {}
                     :denorm-targets {}
                     :denorm-sources {}}
                    spec)]
    (s/validate EntitySchema spec)
    (strict-map->Entity spec)))

(defmacro defentity
  [name entity-spec]
  `(def ~name (create-entity ~entity-spec)))

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

(defn run-save-callbacks
  "run callbacks for an op which requires the old-record"
  [session ^Entity entity callback-key old-record record opts]
  (assert (#{:serialize :before-save :after-save} callback-key))
  (let [all-callbacks (concat (get-in entity [:callbacks callback-key])
                              (get-in opts [callback-key]))
        callback-mfs (for [cb all-callbacks]
                       (fn [record]
                         (cond
                           (fn? cb)
                           (cb record)

                           (satisfies? ICallback cb)
                           (case callback-key
                             ;; it's deliberate -before-save doesn't get the session -
                             ;; it's used internally during upsert, and persistence
                             ;; ops would be bad
                             :before-save
                             (-before-save cb entity old-record record opts)
                             :serialize
                             (-serialize cb entity old-record record opts)
                             :after-save
                             (-after-save cb session entity old-record record opts))

                           :else
                           (throw
                            (ex-info
                             "neither an fn or an ICallback"
                             {:entity entity
                              :callback-key callback-key
                              :callback cb})))))]
    (with-context deferred-context
      (if (not-empty callback-mfs)
        (apply >>= (return record) callback-mfs)
        (return record)))))

(defn chain-save-callbacks
  [session ^Entity entity callback-keys old-record record opts]
  (let [rsf-mfs (for [cbk callback-keys]
                  (fn [record]
                    (run-save-callbacks
                     session
                     entity
                     cbk
                     old-record
                     record
                     opts)))]
    (with-context deferred-context
      (if (not-empty rsf-mfs)
        (apply >>= (return record) rsf-mfs)
        (return record)))))

(defn run-callbacks
  "run callbacks for an op which doesn't require the old-record"
  [session ^Entity entity callback-key record opts]
  (assert (#{:deserialize :after-load :before-delete :after-delete} callback-key))
  (let [all-callbacks (concat (get-in entity [:callbacks callback-key])
                              (get-in opts [callback-key]))
        callback-mfs (for [cb all-callbacks]
                       (fn [record]
                         (cond
                           (fn? cb)
                           (cb record)

                           (satisfies? ICallback cb)
                           (case callback-key
                             :deserialize
                             (-deserialize cb session entity record opts)
                             :after-load
                             (-after-load cb session entity record opts)
                             ;; it's deliberate -before-delete doesn't get the session
                             :before-delete
                             (-before-delete cb entity record opts)
                             :after-delete
                             (-after-delete cb session entity record opts))

                           :else
                           (throw
                            (ex-info
                             "neither an fn or an ICallback"
                             {:entity entity
                              :callback-key callback-key
                              :callback cb})))))]
    (with-context deferred-context
      (if (not-empty callback-mfs)
        (apply >>= (return record) callback-mfs)
        (return record)))))

(defn chain-callbacks
  [session ^Entity entity callback-keys record opts]
  (let [rc-mfs (for [cbk callback-keys]
                 (fn [record]
                   (run-callbacks
                    session
                    entity
                    cbk
                    record
                    opts)))]
    (with-context deferred-context
      (if (not-empty rc-mfs)
        (apply >>= (return record) rc-mfs)
        (return record)))))

(defn create-protect-columns-callback
  "create a callback which will remove cols from a record
   unless the confirm-col is set and non-nil. always
   removes confirm-col"
  [confirm-col & cols]
  (reify
    ICallback
    (-before-save [_ entity old-record record opts]
      (cond
        (::skip-protect opts) (dissoc record confirm-col)
        (get record confirm-col) (dissoc record confirm-col)
        :else (apply dissoc record confirm-col cols)))))

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
    (if (contains? r col)
      (update r col f)
      r)))
