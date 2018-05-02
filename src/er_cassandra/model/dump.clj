(ns er-cassandra.model.dump
  (:require
   [cats.core :as monad :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [cognitect.transit :as transit]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [er-cassandra.record :as cass.r]
   [er-cassandra.model :as cass.m]
   [er-cassandra.session :as cass.session]
   [er-cassandra.schema :as cass.schema]
   [prpr.promise :as prpr :refer [ddo]]
   [prpr.stream :as prpr.stream]
   [manifold.deferred :as d]
   [manifold.stream :as stream]
   [qbits.hayt :as h]
   [taoensso.timbre :as timbre :refer [info]]
   [taoensso.timbre :refer [warn]]
   [er-cassandra.dump.tables :as dump.tables]
   [prpr.stream :as prpr.stream]))

(defn dump-entity
  "dump the primary tables for an entity"
  [cassandra
   keyspace
   directory
   entity]
  (let [table (-> entity :primary-table :name)]
    (dump.tables/dump-table
     cassandra
     keyspace
     directory
     table)))

(defn dump-entities
  "dump the primary tables for a list of entities"
  [cassandra
   keyspace
   directory
   entities]
  (let [tables (map #(get-in % [:primary-table :name]) entities)]
    (dump.tables/dump-tables
     cassandra
     keyspace
     directory
     tables)))

(defn truncate-all-entity-tables
  "truncate all tables of an entity"
  [cassandra
   entity]
  (ddo [:let [keyspace (-> cassandra :alia-session :keyspace)
              entity-table-s (-> [(get-in entity [:primary-table :name])]
                                 (into (->> (:secondary-tables entity)
                                            (remove :view?)
                                            (map :name)))
                                 (into (->> (:lookup-tables entity)
                                            (remove :view?)
                                            (map :name)))
                                 (into (->> (:unique-key-tables entity)
                                            (remove :view?)
                                            (map :name)))
                                 (stream/->source))]]
    (->> entity-table-s
         (stream/map (fn [table]
                       (cass.session/execute
                        cassandra
                        (h/truncate
                         (dump.tables/keyspace-table-name keyspace table)) {})))
         (prpr.stream/count-all-throw))))

(defn load-record-s->entity
  "load a stream of records to an entity"
  [cassandra
   entity
   {notify-s :notify-s
    notify-cnt :notify-cnt
    :as opts}
   r-s]
  (ddo [:let [primary-table (get-in entity [:primary-table :name])
              notify-cnt (or notify-cnt 10000)
              counter-a (atom 0)

              update-counter-fn (fn [cnt]
                                  (let [nc (inc cnt)]
                                    (when (and
                                           notify-s
                                           (= 0 (mod nc notify-cnt)))
                                      (stream/try-put!
                                       notify-s
                                       [primary-table nc]
                                       0))
                                    nc))]

        ;; truncating means we can avoid inserting any null columns
        ;; and avoid creating lots of tombstones
        _ (truncate-all-entity-tables
           cassandra
           entity)

        total-cnt (->> r-s
                       (stream/buffer 50)
                       (stream/map
                        (fn [r]
                          (swap! counter-a update-counter-fn)

                          (cass.m/change
                           cassandra
                           entity
                           [nil r])))
                       (stream/realize-each)
                       (prpr.stream/count-all-throw))]

    (when notify-s
      (stream/put! notify-s [primary-table total-cnt :drained])
      (stream/close! notify-s))

    (return total-cnt)))

(defn load-entity
  "load an entity from a dump of its primary table"
  [cassandra
   directory
   entity]
  (let [table (-> entity :primary-table :name)
        keyspace (-> cassandra :alia-session :keyspace)
        r-s (dump.tables/table->record-s
             keyspace
             directory
             table)]
    (load-record-s->entity
     cassandra
     entity
     {:notify-s (dump.tables/log-notify-stream)}
     r-s)))

(defn load-entities
  "load a list of entities from dumps of their primary tables"
  [cassandra
   directory
   entities]
  (->> entities
       (stream/->source)
       (stream/buffer 5)
       (stream/map #(load-entity
                     cassandra
                     directory
                     %))
       (prpr.stream/count-all-throw
        "er-cassandra.model.dump/load-entities")))
