(ns er-cassandra.model.callbacks.lock-version-callback
  (:require
   [clj-uuid :as uuid]))

(defn create-lock-version-callback
  "moves the provided :lock_version to :lock_version_check
   and creates a new :lock_version uuid... the :lock_version_check
   value can be used with an LWT for optimistic locking"
  []
  (fn [r]
    (-> r
        (assoc :lock_version_check (:lock_version r))
        (assoc :lock_version (uuid/v1)))))
