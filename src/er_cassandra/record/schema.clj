(ns er-cassandra.record.schema
  (:require
   [schema.core :as s])
  (:import
   [qbits.hayt.cql CQLRaw CQLFn]))

(s/defschema ConsistencyLevelSchema
  (s/enum :each-quorum
          :one
          :local-quorum
          :quorum
          :three
          :all
          :serial
          :two
          :local-serial
          :local-one
          :any))

(s/defschema WhereClauseSchema
  [(s/one (s/enum :<= :< := :>= :> :in) :where-op)
   (s/one s/Keyword :where-col)
   (s/one s/Any :where-val)])

(s/defschema WhereSchema
  [WhereClauseSchema])

(s/defschema UpdateColumnSchema
  s/Keyword)

(s/defschema UpdateColumnsSchema
  [(s/one UpdateColumnSchema :first-col) UpdateColumnSchema])

(s/defschema SelectColumnSchema
  (s/conditional
   keyword? s/Keyword
   #(instance? CQLRaw %) CQLRaw
   #(instance? CQLFn %) CQLFn))

(s/defschema SelectColumnsSchema
  [(s/one SelectColumnSchema :first-col) SelectColumnSchema])

(s/defschema OrderByClauseSchema
  [(s/one s/Keyword :order-by-col)
   (s/one (s/enum :asc :desc) :order-by-dir)])

(s/defschema OrderBySchema
  [OrderByClauseSchema])

(s/defschema LimitSchema s/Int)

(s/defschema FullTableSelectOptsSchema
  {(s/optional-key :columns) SelectColumnsSchema
   (s/optional-key :limit) LimitSchema})

(s/defschema SelectOptsSchema
  (merge
   FullTableSelectOptsSchema
   {(s/optional-key :where) WhereSchema
    (s/optional-key :order-by) OrderBySchema
    (s/optional-key :prepare?) s/Bool}))

(s/defschema SelectBufferedOptsSchema
  (merge
   SelectOptsSchema
   {(s/optional-key :fetch-size) s/Int
    (s/optional-key :buffer-size) s/Int}))

(s/defschema PartitionKeySchema
  (s/conditional
   keyword?
   s/Keyword

   :else
   [(s/one s/Keyword :partition-key-first-component)
    s/Keyword]))

(s/defschema KeySchema
  (s/conditional
   keyword?
   s/Keyword

   :else
   [(s/one PartitionKeySchema :partition-key)
    s/Keyword]))

(s/defschema KeyValueComponentSchema
  (s/pred some? :key-value-component))

(s/defschema RecordSchema
  {s/Keyword s/Any})

(s/defschema RecordOrKeyValueSchema
  (s/conditional
   map?
   RecordSchema

   sequential?
   [(s/one KeyValueComponentSchema :first-key-value-component)
    KeyValueComponentSchema]

   :else
   KeyValueComponentSchema))


(s/defschema UpsertUsingSchema
  {(s/optional-key :ttl) s/Int
   (s/optional-key :timestamp) s/Int})

(s/defschema InsertOptsSchema
  {(s/optional-key :if-not-exists) s/Bool
   (s/optional-key :using) UpsertUsingSchema
   (s/optional-key :consistency) ConsistencyLevelSchema})


(s/defschema UpdateOptsSchema
  {(s/optional-key :only-if) WhereSchema
   (s/optional-key :if-exists) s/Bool
   (s/optional-key :if-not-exists) s/Bool
   (s/optional-key :using) UpsertUsingSchema
   (s/optional-key :consistency) ConsistencyLevelSchema
   (s/optional-key :set-columns) UpdateColumnsSchema})

(s/defschema DeleteUsingSchema
  {(s/optional-key :timestamp) s/Int})

(s/defschema DeleteOptsSchema
  {(s/optional-key :only-if) WhereSchema
   (s/optional-key :if-exists) s/Bool
   (s/optional-key :using) DeleteUsingSchema
   (s/optional-key :where) WhereSchema})
