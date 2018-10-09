(ns er-cassandra.model.callbacks.version-callback
  (:require
   [clj-uuid :as uuid])
  (:import
   [er_cassandra.model.callbacks.protocol ICallback]))

(defn create-version-timeuuid-callback
  ([]
   (create-version-timeuuid-callback :version))
  ([col-name]
   (reify
     ICallback
     (-before-save [_ _ _ record opts]
       (assoc record col-name (uuid/v1))))))

