(ns er-cassandra.model.callbacks.timestamp-boolean-callback
  (:require
   [clj-time.core :as time]
   [clj-time.coerce :as time.coerce]
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback
            -before-save]]))

(defn timestamp-boolean-callback
  "a before-save callback that will update `timestamp-col` with the current time
  if `bool-flag-col` is truthy and `timestamp-col` is not set

  conversely the callback will _clear_ `timestamp-col` if it is set and
  `bool-flag-col` is not truthy"
  [bool-flag-col timestamp-col]
  (reify
    ICallback
    (-before-save [_ entity old-record record opts]
      (let [{new-bool-flag bool-flag-col
             cur-timestamp timestamp-col} record
            has-flag? (contains? record bool-flag-col)
            set-timestamp? (and new-bool-flag
                                (nil? cur-timestamp))
            clear-timestamp? (and (not new-bool-flag)
                                  (some? cur-timestamp))]
        (cond
          (not has-flag?)
          record

          set-timestamp?
          (assoc
           record
           timestamp-col (time.coerce/to-date (time/now)))

          clear-timestamp?
          (assoc
           record
           timestamp-col nil)

          :else
          record)))))
