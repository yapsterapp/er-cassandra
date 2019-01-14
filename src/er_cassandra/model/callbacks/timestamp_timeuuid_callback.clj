(ns er-cassandra.model.callbacks.timestamp-timeuuid-callback
  (:require
   [clj-time.coerce :as time.coerce]
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback
            -before-save]])
  (:import
   [com.datastax.driver.core.utils UUIDs]))

(defn timeuuid-from-timestamp-callback
     "a before-save callback that will update `timeuuid-col` with the
     corresponding V1 UUID to the timestamp value from `timestamp-col`
     - but only if the timestamp has changed or the UUID isn't currently set"
     [timestamp-col timeuuid-col]
     (reify
       ICallback
       (-before-save [_ entity old-record record opts]
         (let [{old-timestamp timestamp-col} old-record
               {new-timestamp timestamp-col
                cur-timeuuid timeuuid-col} record
               set-timeuuid? (and new-timestamp
                                  (or (nil? cur-timeuuid)
                                      (not= old-timestamp new-timestamp)))
               new-timeuuid (when set-timeuuid?
                              (-> new-timestamp
                                  time.coerce/to-long
                                  UUIDs/endOf))]
           (if new-timeuuid
             (assoc record timeuuid-col new-timeuuid)
             record)))))

(defn timestamp-from-timeuuid-callback
  "a before-save callback that will update `timestamp-col` with the
  corresponding timestamp to the V1 UUID value from `timeuuid-col`
  - but only if the UUID has changed or the timestamp isn't currently set"
  [timeuuid-col timestamp-col]
  (reify
    ICallback
    (-before-save [_ entity old-record record opts]
      (let [{old-timeuuid timeuuid-col} old-record
            {new-timeuuid timeuuid-col
             cur-timestamp timestamp-col} record
            set-timestamp? (and new-timeuuid
                                (or (nil? cur-timestamp)
                                    (not= old-timeuuid new-timeuuid)))
            new-timestamp (when set-timestamp?
                            (-> new-timeuuid
                                UUIDs/unixTimestamp
                                time.coerce/to-date))]
        (if new-timestamp
          (assoc record timestamp-col new-timestamp)
          record)))))
