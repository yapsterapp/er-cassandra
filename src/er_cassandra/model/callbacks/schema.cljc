(ns er-cassandra.model.callbacks.schema
  (:require
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback]]
   [schema.core :as s]))

(s/defschema CallbackFnSchema
  (s/make-fn-schema s/Any [[{s/Keyword s/Any}]]))

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
