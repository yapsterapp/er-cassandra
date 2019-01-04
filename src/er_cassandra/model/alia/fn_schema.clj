(ns er-cassandra.model.alia.fn-schema
  (:require
   [clojure.set :as set]
   [schema.core :as s]
   [er-cassandra.record.schema :as rs]
   [manifold.deferred :as d]))

(s/defschema UpsertConsistencySchema
  {(s/optional-key :consistency)
   rs/ConsistencyLevelSchema})

(s/defschema UpsertWhereSchema
  {(s/optional-key :where) rs/WhereSchema})

(s/defschema UpsertUsingSchema
  {(s/optional-key :using) rs/UpsertUsingSchema})

(s/defschema UpsertInsertSchema
  {(s/optional-key :if-not-exists) s/Bool})

(s/defschema UpsertUpdateSchema
  {(s/optional-key :only-if) rs/WhereSchema
   (s/optional-key :if-exists) s/Bool})

(s/defschema UpsertSkipProtectSchema
  {(s/optional-key :er-cassandra.model.types/skip-protect) s/Bool})

(s/defschema UpsertSkipDenormalizeSchema
  {(s/optional-key :er-cassandra.model.types/skip-denormalize) #{s/Keyword}})

(s/defschema MinimalChangeSchema
  {(s/optional-key :er-cassandra.model.types/minimal-change) s/Bool})

(s/defschema UpsertControlSchema
  (merge
   UpsertSkipProtectSchema
   UpsertSkipDenormalizeSchema
   MinimalChangeSchema))

(defn has-some-key?
  "returns an fn which tests whether its argument
   is a map with one or more keys from key-seq"
  [& key-seq]
  (let [ks (set key-seq)]
    (fn [m]
      (and (map? m)
           (not-empty
            (set/intersection
             ks
             (set (keys m))))))))

(defn conditional-upsert-schema
  "applies rules about INSERT and UPDATE LWT statements -
   they can't exist together"
  [common-schema]
  (s/conditional
   :if-not-exists
   (merge common-schema UpsertInsertSchema)

   (has-some-key? :if-exists :only-if)
   (merge common-schema UpsertUpdateSchema)

   :else
   common-schema))

(s/defschema UpsertOptsSchema
  (conditional-upsert-schema
   (merge
    rs/PrepareOptSchema
    UpsertConsistencySchema
    UpsertWhereSchema
    UpsertUsingSchema
    UpsertControlSchema)))

(s/defschema UpsertUsingWithTimestampSchema
  (merge
   UpsertControlSchema
   {(s/optional-key :ttl) s/Int
    :timestamp s/Int}))

(s/defschema UpsertUsingOnlyOptsWithTimestampSchema
  (merge
   rs/PrepareOptSchema
   UpsertConsistencySchema
   {:using UpsertUsingWithTimestampSchema}
   UpsertControlSchema))

(s/defschema UpsertOptsWithTimestampSchema
  (conditional-upsert-schema
   (merge
    rs/PrepareOptSchema
    UpsertConsistencySchema
    UpsertWhereSchema
    UpsertUsingOnlyOptsWithTimestampSchema
    UpsertControlSchema)))

(s/defschema DeleteUsingWithTimestampSchema
  {:timestamp s/Int})

(s/defschema DeleteOptsWithTimestampSchema
  (-> rs/DeleteOptsSchema
      (dissoc (s/optional-key :using))
      (assoc :using DeleteUsingWithTimestampSchema)))

(s/defschema DeleteUsingOnlyOptsWithTimestampSchema
  (merge
   rs/PrepareOptSchema
   rs/ConsistencyOptSchema
   {:using DeleteUsingWithTimestampSchema}))

(s/defn upsert-opts->delete-opts :- rs/DeleteOptsSchema
  "remove irrelevant parts of upsert opts to match delete opts"
  [{using :using :as upsert-opts} :- UpsertOptsSchema]
  (-> upsert-opts
      (dissoc :if-not-exists)
      (dissoc :consistency)
      (dissoc :er-cassandra.model.types/skip-denormalize)
      (dissoc :er-cassandra.model.types/skip-protect)
      (dissoc :er-cassandra.model.types/minimal-change)
      (merge
       (when (map? using)
         {:using (dissoc using :ttl)}))))

(s/defn upsert-opts->insert-opts :- rs/InsertOptsSchema
  "remove irrelevant parts of upsert opts to match insert opts"
  [upsert-opts :- UpsertOptsSchema]
  (-> upsert-opts
      (dissoc :where)
      (dissoc :only-if)
      (dissoc :if-exists)
      (dissoc :er-cassandra.model.types/skip-denormalize)
      (dissoc :er-cassandra.model.types/skip-protect)
      (dissoc :er-cassandra.model.types/minimal-change)))

(s/defn primary-upsert-opts->lookup-delete-opts
  "given upsert opts for a primary table, return suitable delete
   opts for a lookup/secondary record"
  [{using :using :as upsert-opts} :- UpsertOptsWithTimestampSchema]
  (-> upsert-opts
      (select-keys [:using :prepare :consistency])
      (merge
       (when (map? using)
         {:using (dissoc using :ttl)}))))

(s/defn primary-upsert-opts->lookup-upsert-opts
  "given upsert opts for a primary table, return suitable upsert
   opts for a lookup/secondary record"
  [upsert-opts :- UpsertOptsWithTimestampSchema]
  (select-keys upsert-opts
               [:using
                :prepare
                :consistency
                :er-cassandra.model.types/minimal-change]))

(s/defn upsert-opts->using-only :- UpsertUsingOnlyOptsWithTimestampSchema
  "pick out just the :using opts"
  [upsert-opts :- UpsertOptsWithTimestampSchema]
  (select-keys upsert-opts [:using]))

(defn opts-remove-timestamp
  [opts]
  (update-in opts [:using] (fn [u] (dissoc u :timestamp))))

(s/defschema DenormalizeCallbackOptsSchema
  (merge
   UpsertSkipDenormalizeSchema
   {(s/optional-key :fetch-size) s/Int
    (s/optional-key :buffer-size) s/Int}))

(s/defschema DenormalizeOptsSchema
  (merge
   rs/PrepareOptSchema
   UpsertUsingOnlyOptsWithTimestampSchema
   DenormalizeCallbackOptsSchema))

(s/defn denormalize-opts->upsert-opts :- UpsertOptsSchema
  [opts :- DenormalizeOptsSchema]
  (dissoc opts :fetch-size :buffer-size))

(s/defn upsert-opts->denormalize-opts :- DenormalizeOptsSchema
  [upsert-opts :- UpsertOptsSchema]
  (-> upsert-opts
      (dissoc :where)
      (dissoc :consistency)))

(s/defn denormalize-opts->delete-opts :- rs/DeleteOptsSchema
  [opts :- DenormalizeOptsSchema]
  (-> opts
      denormalize-opts->upsert-opts
      upsert-opts->delete-opts))

(s/defn delete-opts->denormalize-opts :- DenormalizeOptsSchema
  [delete-opts :- rs/DeleteOptsSchema]
  (-> delete-opts
      (dissoc :where)
      (dissoc :if-exists)
      (dissoc :only-if)))

(s/defn opts->prepare?-opt :- rs/PrepareOptSchema
  [opts]
  (select-keys opts [:prepare?]))
