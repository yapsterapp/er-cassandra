(ns er-cassandra.model.callbacks.protocol)

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
