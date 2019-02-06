(ns er-cassandra.model.callbacks
  (:require
   #?@(:clj [[cats.core :refer [mlet return >>=]]
             [cats.context :refer [with-context]]
             [cats.labs.manifold :refer [deferred-context]]])
   #?@(:clj  [[clj-time.core :as time]
              [clj-time.coerce :as time.coerce]]
       :cljs [[cljs-time.core :as time]
              [cljs-time.coerce :as time.coerce]])
   [er-cassandra.model.types :as t]
   [er-cassandra.model.callbacks.protocol
    :refer [ICallback
            -before-delete
            -after-delete
            -after-load
            -before-save
            -after-save
            -deserialize
            -serialize]]
   [taoensso.timbre :refer [info]])
  #?(:clj
     (:import
      [er_cassandra.model.types Entity])))

(defn create-protect-columns-callback
  "create a callback which will replace cols from a record
   with their old-record values, unless the confirm-col is
   set and non-nil. always removes confirm-col"
  [confirm-col & cols]
  (reify
    ICallback
    (-before-save [_ entity old-record record opts]
      (cond
        (::t/skip-protect opts) (dissoc record confirm-col)
        (get record confirm-col) (dissoc record confirm-col)
        :else (merge
               record
               (->> (for [c cols]
                      [c (get old-record c)])
                    (into {})))))))

(defn create-updated-at-callback
  "create a callback which will set an :updated_at column
   - but only if there are changes - it won't trigger any change itself"
  ([] (create-updated-at-callback :updated_at))
  ([updated-at-col]
   (reify
     ICallback
     (-before-save [_ entity old-record record opts]
       (if (or (nil? old-record)
               (not= (select-keys old-record (keys record))
                     (into {} record)))
         (assoc record updated-at-col (time.coerce/to-date (time/now)))
         record)))))

(defn create-select-view-callback
  "selects the given columns from a record"
  [cols]
  (fn [r]
    (select-keys r cols)))

(defn create-filter-view-callback
  "filers the given columns from a record"
  [cols]
  (fn [r]
    (apply dissoc r cols)))

(defn create-update-col-callback
  "a callback which updates a column with a function"
  [col f]
  (fn [r]
    (if (contains? r col)
      (update r col f)
      r)))

#?(:clj
   (defn run-save-callbacks
     "run callbacks for an op which requires the old-record"
     [session ^Entity entity callback-key old-record record opts]
     (assert (#{:serialize :before-save :after-save} callback-key))
     (let [all-callbacks (concat (get-in entity [:callbacks callback-key])
                                 (get-in opts [callback-key]))
           callback-mfs (for [cb all-callbacks]
                          (fn [record]
                            (cond
                              (fn? cb)
                              (cb record)

                              (satisfies? ICallback cb)
                              (case callback-key
                                ;; it's deliberate -before-save doesn't get the session -
                                ;; it's used internally during upsert, and persistence
                                ;; ops would be bad
                                :before-save
                                (-before-save cb entity old-record record opts)
                                :serialize
                                (-serialize cb entity old-record record opts)
                                :after-save
                                (-after-save cb session entity old-record record opts))

                              :else
                              (throw
                               (ex-info
                                "neither an fn or an ICallback"
                                {:entity entity
                                 :callback-key callback-key
                                 :callback cb})))))]
       (with-context deferred-context
         (if (not-empty callback-mfs)
           (apply >>= (return record) callback-mfs)
           (return record))))))

#?(:clj
   (defn chain-save-callbacks
     [session ^Entity entity callback-keys old-record record opts]
     (let [rsf-mfs (for [cbk callback-keys]
                     (fn [record]
                       (run-save-callbacks
                        session
                        entity
                        cbk
                        old-record
                        record
                        opts)))]
       (with-context deferred-context
         (if (not-empty rsf-mfs)
           (apply >>= (return record) rsf-mfs)
           (return record))))))

#?(:clj
   (defn run-callbacks
     "run callbacks for an op which doesn't require the old-record"
     [session ^Entity entity callback-key record opts]
     (assert (#{:deserialize :after-load :before-delete :after-delete} callback-key))
     (let [all-callbacks (concat (get-in entity [:callbacks callback-key])
                                 (get-in opts [callback-key]))
           callback-mfs (for [cb all-callbacks]
                          (fn [record]
                            (cond
                              (fn? cb)
                              (cb record)

                              (satisfies? ICallback cb)
                              (case callback-key
                                :deserialize
                                (-deserialize cb session entity record opts)
                                :after-load
                                (-after-load cb session entity record opts)
                                ;; it's deliberate -before-delete doesn't get the session
                                :before-delete
                                (-before-delete cb entity record opts)
                                :after-delete
                                (-after-delete cb session entity record opts))

                              :else
                              (throw
                               (ex-info
                                "neither an fn or an ICallback"
                                {:entity entity
                                 :callback-key callback-key
                                 :callback cb})))))]
       (with-context deferred-context
         (if (not-empty callback-mfs)
           (apply >>= (return record) callback-mfs)
           (return record))))))

#?(:clj
   (defn chain-callbacks
     [session ^Entity entity callback-keys record opts]
     (let [rc-mfs (for [cbk callback-keys]
                    (fn [record]
                      (run-callbacks
                       session
                       entity
                       cbk
                       record
                       opts)))]
       (with-context deferred-context
         (if (not-empty rc-mfs)
           (apply >>= (return record) rc-mfs)
           (return record))))))
