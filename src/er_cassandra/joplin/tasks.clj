(ns er-cassandra.joplin.tasks
  (:require [ragtime.protocols :refer [DataStore]]
            [ragtime.repl :as rr]
            [ragtime.strategy :as strategy]
            [clj-time.core :as t]
            [clj-time.format :as t.f]
            [clj-time.coerce :as t.c]
            [qbits.hayt :as hayt]
            [joplin.core :as jcore]
            [er-cassandra.joplin.migration-helpers :as jmh]
            [er-cassandra.model.model-session :as ms]
            [taoensso.timbre :refer [trace debug info warn error]])
            [er_cassandra.model.alia.model-session]
            [er-cassandra.record :as cass.r]
            [cats.core :as monad]
            [prpr.promise :as promise :refer [ddo]]
            [prpr.stream :as stream])
  (:import (er_cassandra.model.alia.model_session AliaModelSession
                                                  AliaModelSpySession)))

(def migrator-formatter (t.f/formatter "YYYYMMddHHmmss"))
(def migrations-table :migrations_v2)
(def table-namespace "er-cassandra.joplin")

(defn cass->alia-session
  "Takes as its argument the cassandra entry from the system map
   which will be an AMS or AMSS (see imports above) and returns
   the underlying alia session"
  [cass]
  (let [alia-session (ms/-record-session cass)]
    (println "> cass->alia-session")
    (println alia-session)
    alia-session))

(defn get-full-migrator-id
  "Get a string with current date and time prepended"
  [id]
  (let [time-formatter (partial f/unparse migrator-formatter)]
    (-> (t/now)
        time-formatter
        (str "-" id))))

(defn migrator-id->created-at
  "Synthesise a created at from a migrator ID"
  [id]
  (->> (clojure.string/split id #"-")
       first))

(defn ensure-migration-schema
  "Ensures the migration schema is loaded"
  [session]
  (jmh/execute session
               (hayt/create-table migrations-table
                                  (hayt/if-not-exists)
                                  (hayt/column-definitions {:id :varchar
                                                            :namespace :varchar
                                                            :primary-key [:namespace :id]}))))

(defn insert-into-new-migrations-table
  "A helper fn that does exactly what it says on the tin"
  [session {migration-id :id
            namespace :namespace}]
  (jmh/execute session
               (hayt/insert migrations-table
                            (hayt/values {:id migration-id
                                          :namespace namespace}))))

(defn copy-data-from-migrations-table-v1-to-v2
  "We want to copy the old migrations table and insert it
   into the new table to make this forward compatible.
   We know there are about twenty migrations tops, as up to 2.5.0
   is arrived at by a single migration.
   This fn assumes the session is created by migration helpers -
   i.e. that it is env dependent"
  [session]
  (let [existing-migrations (jmh/execute session
                                         (hayt/select "migrations"))
        existing-migrations? (not (empty? existing-migrations))]
    (if existing-migrations?
      (do
        (ensure-migration-schema session)
        (doseq [{migration-id :id} existing-migrations]
          (insert-into-new-migrations-table session
                                            {:id migration-id
                                             :namespace table-namespace})))
      (error "No existing migrations found"))))

(defn migration-exists?
  "Checks that a migration exists in the DB
   used to validate that Bad Things(tm)
   won't happen on rollback"
  [session migration-id]
  (not (empty? (jmh/execute session
                            (hayt/select migrations-table
                                         (hayt/where {:namespace table-namespace
                                                      :id migration-id}))))))

;; migration helpers
(defn- -add-migration-id [this id]
  (ensure-migration-schema this)
  (insert-into-new-migrations-table this
                                    {:id id
                                     :namespace table-namespace}))

(defn- -remove-migration-id [this id]
  (ensure-migration-schema this)
  (jmh/execute this
               (hayt/delete migrations-table
                            (hayt/where {:namespace table-namespace
                                         :id id}))))

(defn- -applied-migration-ids [this]
  (ensure-migration-schema this)
  (->> (jmh/execute this
                    (hayt/select migrations-table
                                 (hayt/where {:namespace table-namespace})
                                 (hayt/order-by [:id :asc])))
       (map :id)))

(defn last-migration
  "Returns the record of the last (latest) migration applied or nil"
  [sess]
  (-> sess
      (jmh/execute (hayt/select migrations-table
                                (hayt/where {:namespace table-namespace})
                                (hayt/order-by [:id :desc])
                                (hayt/limit 1)))
      (first)))



;; an alia SessionManager instance is required for joplin
;; we want to use our own constructs instead
;; https://github.com/juxt/joplin/blob/master/joplin.cassandra/src/joplin/cassandra/database.clj#L32
(extend-protocol DataStore

  AliaModelSession
  (add-migration-id [this id]
    (-add-migration-id this id))
  (remove-migration-id [this id]
    (-remove-migration-id this id))
  (applied-migration-ids [this]
    (-applied-migration-ids this))

  AliaModelSpySession
  (add-migration-id [this id]
    (-add-migration-id this id))
  (remove-migration-id [this id]
    (-remove-migration-id this id))
  (applied-migration-ids [this]
    (-applied-migration-ids this)))

(defn cass-migrate
  "Run all pending migrations in the migration-files-path
   cass-session should be deref'd before pasing in as this
   is synchronous"
  [cass-session migration-files-path env & [args]]
  (do
    (info "Migrating " cass-session)
    (rr/migrate {:datastore cass-session
                 :migrations (jcore/get-migrations migration-files-path)})))

(defn cass-migrate-one
  "Run a single migration. Use with care.
   Runs the migration regardless of whether
   or not it comes before already applied migrations"
  [cass-session migration-files-path env migration-id & args]
  (let [migrations (jcore/get-migrations migration-files-path)
        migration-id->migration (->> migrations
                                     (filter #(= migration-id
                                                 (:id %))))
        migration-exists? (not (nil? (first migration-id->migration)))]
    (if migration-exists?
      (rr/migrate {:datastore cass-session
                   :migrations migration-id->migration
                   :strategy strategy/apply-new})
      (error (str "Can't migrate up - target migration "
                  migration-id
                  " not found.")))))

(defn cass-migrate-to
  "Migrate UP to the specified migration id
   gets unapplied migrations, up to the ID in question"
  [cass-session migration-files-path env migration-id]
  (let [migrations (jcore/get-migrations migration-files-path)
        target-migration (->> migrations
                              (filter #(= migration-id
                                          (:id %)))
                              first)
        migrations-excl-target (->> (take-while #(not (= migration-id
                                                         (:id %)))
                                                migrations)
                                    (into []))
        pending-migrations (conj migrations-excl-target
                                 target-migration)
        target-migration-exists? (not (nil? target-migration))]
    (if target-migration-exists?
      (rr/migrate {:datastore cass-session
                   :migrations pending-migrations})
      (error (str "Can't migrate up - target migration "
                  migration-id
                  " not found.")))))

(defn cass-rollback
  "Takes either the number of down migrations to apply
   or the migration id to roll down to.
   The migration id is the timestamp AND string identifier
   found in the migration namespace and filename for the migration"
  [cass-session migration-files-path env migration-id]
  (let [id-valid? (migration-exists? cass-session migration-id)]
    (if id-valid?
      (do
        (info "Rolling back to " migration-id)
        (rr/rollback {:datastore cass-session
                      :migrations (jcore/get-migrations migration-files-path)}
                     migration-id))
      (error (str "Can't rollback - migration "
                  migration-id
                  " not found.")))))

(defn cass-rollback-last
  "Rollback the last migration (optionally last n migrations)"
  ([cass-session migration-files-path]
   (cass-rollback-last cass-session migration-files-path 1))
  ([cass-session migration-files-path n]
   (assert (int? n))
   (assert (pos? n))
   (do
     (info "Rolling back last migrations:" n)
     (if (some? (last-migration cass-session))
       (rr/rollback {:datastore cass-session
                     :migrations (jcore/get-migrations migration-files-path)}
                    n)
       (info "No migrations to rollback")))))

(defn cass-seed
  "Not yet needed"
  [])

(defn cass-pending-migrations
  "Not yet needed. Would expose pending but unapplied migrations"
  [])

;; implement cass-create-migration in your project
;; with an appropriate target and scaffold.

(def id-timestamp-formatter (t.f/formatter "YYYYMMddHHmmss"))

(defn fix-migration-timestamp
  [{id :id
    created-at :created_at
    :as migration}]
  (let [[_ id-timestamp] (re-matches #"(\d+)-.*" id)
        timestamp (t.f/parse
                   id-timestamp-formatter
                   id-timestamp)]
    (assoc migration
           :created_at
           (t.c/to-date timestamp))))

(defn fix-all-migration-timestamps*
  [cass-session]
  (ddo [migration-s (cass.r/select-buffered
                     cass-session
                     :migrations)]
    (->> migration-s
         (stream/map fix-migration-timestamp)
         (cass.r/insert-buffered
          cass-session
          :migrations)
         (monad/return))))

(defn fix-all-migration-timestamps
  [cass-session]
  (ddo [fix-rs (fix-all-migration-timestamps* cass-session)]
    (stream/count-all-throw
     ::fix-all-migration-timestamps
     fix-rs)))
