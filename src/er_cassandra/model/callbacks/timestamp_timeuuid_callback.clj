(ns er-cassandra.model.callbacks.timestamp-timeuuid-callback
  (:require
   [clj-uuid :as uuid]
   [clj-time.core :as time]
   [clj-time.coerce :as time.coerce]
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback
            -before-save]]
   [taoensso.timbre :refer [info]])
  (:import
   [com.datastax.driver.core.utils UUIDs]))

(defn created-at-timeuuid-callback
  "adds a created-at timeuuid timestamp to a record - ignores
   modificiations to the column (preserving the old value),
   unless explicitly instructed to override with override-col"
  ([created-at-timeuuid-col]
   (created-at-timeuuid-callback created-at-timeuuid-col nil))
  ([created-at-timeuuid-col override-col]
   (let [override-col (or override-col (-> created-at-timeuuid-col
                                           name
                                           (as-> % (str "set-" % "?"))
                                           keyword))]
     (reify
       ICallback
       (-before-save [_ entity old-record record opts]
         (let [{old-timeuuid created-at-timeuuid-col} old-record
               {override? override-col} record

               override-timeuuid? (and (contains? record created-at-timeuuid-col)
                                       (true? override?))

               ;; set only if it's a new record and the column is not
               ;; explicitly set by providing the override
               set-timeuuid? (and
                              (nil? old-record)
                              (not override-timeuuid?))

               new-timeuuid (when set-timeuuid?
                              (uuid/v1))]

           ;; (info "created-at-timeuuid-callback" [old-record record] override-timeuuid? set-timeuuid? new-timeuuid)
           (cond
             set-timeuuid?
             (-> record
                 (dissoc override-col)
                 (assoc created-at-timeuuid-col new-timeuuid))

             override-timeuuid?
             (dissoc record override-col)

             :else
             (-> record
                 (dissoc override-col)
                 (assoc created-at-timeuuid-col old-timeuuid)))))))))

(defn created-at-callback
  "adds a created-at timestamp to a record - ignores
   modificiations to the column (preserving the old value),
   unless explicitly instructed to override with override-col"
  ([created-at-col]
   (created-at-callback created-at-col nil))
  ([created-at-col override-col]
   (let [override-col (or override-col (-> created-at-col
                                           name
                                           (as-> % (str "set-" % "?"))
                                           keyword))]
     (reify
       ICallback
       (-before-save [_ entity old-record record opts]
         (let [{old-created-at created-at-col} old-record
               {override? override-col} record

               override-created-at? (and (contains? record created-at-col)
                                         (true? override?))

               ;; set only if it's a new record and the column is not
               ;; explicitly set by providing the override
               set-created-at? (and
                                (nil? old-record)
                                (not override-created-at?))

               new-created-at (when set-created-at?
                                (-> (time/now)
                                    (time.coerce/to-date)))]
           (cond

             set-created-at?
             (-> record
                 (dissoc override-col)
                 (assoc created-at-col new-created-at))

             override-created-at?
             (dissoc record override-col)

             :else
             (-> record
                 (dissoc override-col)
                 (assoc created-at-col old-created-at)))))))))

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
