(ns er-cassandra.model.alia.fn-schema
  (:require
   [schema.core :as s]
   [er-cassandra.record :as r]))

(s/defschema UpsertOptsSchema
  {(s/optional-key :where) r/WhereSchema
   (s/optional-key :only-if) r/WhereSchema
   (s/optional-key :if-exists) s/Bool
   (s/optional-key :if-not-exists) s/Bool
   (s/optional-key :using) r/UpsertUsingSchema})

(s/defschema UpsertUsingWithTimestampSchema
  {(s/optional-key :ttl) s/Int
   :timestamp s/Int})

(s/defschema UpsertOptsWithTimestampSchema
  (->
   UpsertOptsSchema
   (dissoc (s/optional-key :using))
   (assoc :using UpsertUsingWithTimestampSchema)))

(s/defschema UpsertUsingOnlyOptsWithTimestampSchema
  {:using UpsertUsingWithTimestampSchema})

(s/defschema DeleteUsingWithTimestampSchema
  {:timestamp s/Int})

(s/defschema DeleteOptsWithTimestampSchema
  (-> r/DeleteOptsSchema
      (dissoc (s/optional-key :using))
      (assoc :using DeleteUsingWithTimestampSchema)))

(s/defschema DeleteUsingOnlyOptsWithTimestampSchema
  {:using DeleteUsingWithTimestampSchema})

(s/defn upsert-opts->delete-opts :- r/DeleteOptsSchema
  "remove irrelevant parts of upsert opts to match delete opts"
  [upsert-opts :- UpsertOptsSchema]
  (-> upsert-opts
      (dissoc :if-not-exists)
      (update-in [:using] (fn [u]
                            (dissoc u :ttl)))))

(s/defn upsert-opts->insert-opts :- r/InsertOptsSchema
  "remove irrelevant parts of upsert opts to match insert opts"
  [upsert-opts :- UpsertOptsSchema]
  (-> upsert-opts
      (dissoc :where)
      (dissoc :only-if)
      (dissoc :if-exists)))

(s/defn upsert-opts->using-only :- UpsertUsingOnlyOptsWithTimestampSchema
  "pick out just the :using opts"
  [upsert-opts :- UpsertOptsWithTimestampSchema]
  (select-keys upsert-opts [:using]))

(defn opts-remove-timestamp
  [opts]
  (update-in opts [:using] (fn [u] (dissoc u :timestamp))))

(s/defschema DenormalizeOptsSchema
  (merge
   UpsertUsingOnlyOptsWithTimestampSchema
   {(s/optional-key :fetch-size) s/Int
    (s/optional-key :buffer-size) s/Int}))

(s/defn denormalize-opts->upsert-opts :- UpsertOptsSchema
  [opts :- DenormalizeOptsSchema]
  (dissoc opts :fetch-size :buffer-size))

(s/defn denormalize-opts->delete-opts :- r/DeleteOptsSchema
  [opts :- DenormalizeOptsSchema]
  (-> opts
      denormalize-opts->upsert-opts
      upsert-opts->delete-opts))
