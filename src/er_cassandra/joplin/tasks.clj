(ns er-cassandra.joplin.tasks
  (:require [ragtime.protocols :refer [DataStore]]
            [clj-time.format :as t.f]
            [clj-time.coerce :as t.c]
            [qbits.hayt :as hayt]
            [joplin.core :as jcore]
            [er-cassandra.joplin.migration-helpers :as jmh]
            [er-cassandra.model.model-session :as ms]
            [er_cassandra.model.alia.model-session]
            [er-cassandra.record :as cass.r]
            [cats.core :as monad]
            [prpr.promise :as promise :refer [ddo]]
            [prpr.stream :as stream])
  (:import (er_cassandra.model.alia.model_session AliaModelSession
                                                  AliaModelSpySession)))


(defn cass->alia-session
  "Takes as its argument the cassandra entry from the system map
   which will be an AMS or AMSS (see imports above) and returns
   the underlying alia session"
  [cass]
  (let [alia-session (ms/-record-session cass)]
    (println "> cass->alia-session")
    (println alia-session)
    alia-session))

(defn ensure-migration-schema
  "Ensures the migration schema is loaded"
  [session]
  (jmh/execute session
               (hayt/create-table :migrations
                                  (hayt/if-not-exists)
                                  (hayt/column-definitions {:id :varchar
                                                            :created_at :timestamp
                                                            :primary-key [:id]}))))

;; migration helpers
(defn- -add-migration-id [this id]
  (ensure-migration-schema this)
  (jmh/execute this
               (hayt/insert :migrations
                            (hayt/values {:id id
                                          :created_at (java.util.Date.)}))))
(defn- -remove-migration-id [this id]
  (ensure-migration-schema this)
  (jmh/execute this
               (hayt/delete :migrations
                            (hayt/where {:id id}))))

(defn- -applied-migration-ids [this]
  (ensure-migration-schema this)
  (->> (jmh/execute this
                    (hayt/select :migrations))
       (sort-by :created_at)
       (map :id)))

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
  [cass-session migration-files-path env & args]
  (apply jcore/do-migrate
         (jcore/get-migrations migration-files-path)
         cass-session
         args))

(defn cass-rollback
  "Takes either the number of down migrations to apply
   or the migration id to roll down to.
   The migration id is the timestamp AND string identifier
   found in the migration namespace and filename for the migration"
  [cass-session migration-files-path env num-or-id & args]
  (apply jcore/do-rollback
         (jcore/get-migrations migration-files-path)
         cass-session
         num-or-id
         args))

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
