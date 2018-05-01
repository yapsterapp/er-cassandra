(ns er-cassandra.model.types.change
  (:require
   [clojure.set :as set]
   [prpr.promise :as pr]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.lookup :as lookup]
   [taoensso.timbre :refer [info warn]])
  (:import
   [er_cassandra.model.types Entity]))

(defn additional-cols-for-table
  [^Entity entity
   {t-key :key
    t-type :type
    t-generator-fn :generator-fn
    t-with-cols :with-columns
    :as table}]

  (if t-generator-fn
    :all ;; generator-fn could include anything dynamically
    (case t-type
      :primary :all
      :secondary :all
      :uniquekey t-with-cols
      :lookup t-with-cols)))

(defn change-contribution
  "returns the columns (outside of the uberkey columns)
   required to fully effect any change  contribution for a table

   - nothing if there are no changes
   - if there is any change to key or denormed values then
     all key cols and any changed denormalized value cols"
  [^Entity entity
   old-record
   new-record
   changed-cols-set
   {t-key :key
    t-view? :view?
    :as table}]

  (if t-view?
    nil ;; no changes other than primary table changes required
    (let [uberkey-cols-set (->> (get-in entity [:primary-table :key])
                                flatten
                                set)
          key-cols (->> (:key table)
                        flatten
                        (filter (comp not uberkey-cols-set)))

          extra-cols (additional-cols-for-table entity table)

          key-changes? (some changed-cols-set key-cols)

          ;; key cols with nil values in the old-record
          old-key-nil-col-set (->> key-cols
                                   (filter
                                    #(nil? (get old-record %)))
                                   set)

          ;; key cols with nil values in the new-record
          new-key-nil-col-set (->> key-cols
                                   (filter
                                    #(nil? (get new-record %)))
                                   set)]

      (cond
        (empty? changed-cols-set)
        nil

        ;; if there are nils in the old-record key and new-record-key
        ;; then this table will have no changes
        (and (not-empty old-key-nil-col-set)
             (not-empty new-key-nil-col-set))
        nil

        ;; if the key is changed then denorm cols will be required
        key-changes?
        (if (= :all extra-cols)
          ;; everything is denormed. this is impossible to guarantee
          ;; since we don't know the full column set of the entity,
          ;; but if the old-record has more fields
          ;; than the new record, something is definitely wrong
          ;; TODO query all table columns into the entity model
          ;; for use in this situation
          (do
            (when (> (count old-record) (count new-record))
              (throw
               (pr/error-ex ::all-entity-cols-required-for-denorm
                            {:entity entity
                             :old-record old-record
                             :new-record new-record
                             :table table})))
            (set
             (filter
              (complement uberkey-cols-set)
              (keys new-record))))

          ;; only selected cols are denormed
          (set (concat
                key-cols
                extra-cols)))

        ;; no key change, only denorm changes
        (= :all extra-cols)
        (->> (into changed-cols-set key-cols)
             not-empty)

        ;; no key change, only denorm changes
        (some changed-cols-set extra-cols)
        (->> (filter changed-cols-set extra-cols)
             (into key-cols)
             set
             not-empty)))))

(defn minimal-change-cols
  [^Entity entity old-record new-record]
  (let [uberkey-cols-set (->> (get-in entity [:primary-table :key])
                              flatten
                              set)
        new-record-cols-set (->> new-record keys set)
        old-record-cols-set (->> old-record keys set)

        tables (t/all-entity-tables entity)

        changed-cols-set (->> (keys new-record)
                              (filter (fn [nrk]
                                        (not= (get new-record nrk)
                                              (get old-record nrk))))
                              set)]

    ;; the uberkey columns must always be present
    (when-let [missing-cols (or (and (some? new-record)
                                     (not-empty
                                      (set/difference
                                       uberkey-cols-set
                                       new-record-cols-set)))
                                (and (some? old-record)
                                     (not-empty
                                      (set/difference
                                       uberkey-cols-set
                                       old-record-cols-set)))) ]
      (throw (pr/error-ex ::incomplete-uberkey
                          {:entity entity
                           :old-record old-record
                           :new-record new-record
                           :missing-cols missing-cols})))

    ;; if the old-record is non-nil (i.e. this isn't a create)
    ;; it must be possible to retrieve the previous value of
    ;; every new-record column
    (when-let [missing-cols (and (some? old-record)
                                 (not-empty
                                  (set/difference
                                   new-record-cols-set
                                   old-record-cols-set)))]
      (throw (pr/error-ex ::incomplete-old-record
                          {:entity entity
                           :old-record old-record
                           :new-record new-record
                           :missing-cols missing-cols})))

    ;; if this isn't a create,
    ;; it is never possible to update an uberkey
    (when (and (some? old-record)
               (some #(contains? changed-cols-set %) uberkey-cols-set))
      (throw (pr/error-ex ::uberkey-changed
                          {:entity entity
                           :old-record old-record
                           :new-record new-record})))

    (let [;; these are the non-uberkey cols required to effect the change
          mc-cols (mapcat (partial change-contribution
                                   entity
                                   old-record
                                   new-record
                                   changed-cols-set)
                          tables)]

      ;; if there are no non-uberkey cols required, then there is
      ;; no change
      (if (not-empty mc-cols)
        (into uberkey-cols-set mc-cols)
        nil))))

(defn minimal-change
  "return a record which is the minimal change for the
   requested change - any cols which would have no effect
   (other than to create tombstones or other garbage) will
   be removed

   several types of change error are also detected:

   - changes to any uberkey column values
   - changes to a part of any index-table key where the whole
     index-table key is not present
   - columns missing from old-record (which are present on new-record)
   - columns missing from the new-record such as lookup or secondary
     table key columns which are required because a changed column
     would be denormalized to that table

   the minimal upsert contains all changed columns, plus any key columns
   and denorm columns from any index tables which have any key or denorm
   columns changed"
  [^Entity entity old-record new-record]
  (let [mc-cols-set (minimal-change-cols entity old-record new-record)
        new-record-cols-set (-> new-record keys set)]

    (when-let [missing-cols (and (some? new-record)
                                 (not-empty
                                  (set/difference
                                   mc-cols-set
                                   new-record-cols-set)))]
      (throw
       (pr/error-ex ::missing-columns
                    {:entity entity
                     :old-record old-record
                     :new-record new-record
                     :missing-cols missing-cols})))

    (not-empty
     (select-keys
      new-record
      mc-cols-set))))
