(ns er-cassandra.model.dump
  (:require
   [cats.core :as monad :refer [return]]
   [er-cassandra.model :as cass.m]
   [er-cassandra.model.callbacks :as cass.cb]
   [er-cassandra.session :as cass.session]
   [er-cassandra.model.types :as cass.t]
   [prpr.promise :as prpr :refer [ddo]]
   [prpr.stream :as stream]
   [qbits.hayt :as h]
   [er-cassandra.dump.tables :as dump.tables]
   [er-cassandra.dump.transit :as dump.transit]))



(defn dump-entity
  "dump the primary tables for an entity"
  [cassandra
   directory
   entity]
  (let [keyspace (cass.session/keyspace cassandra)
        table (-> entity :primary-table :name)]
    (dump.tables/dump-table
     cassandra
     keyspace
     directory
     table)))

(defn dump-entities
  "dump the primary tables for a list of entities"
  [cassandra
   directory
   entities]
  (let [keyspace (cass.session/keyspace cassandra)
        tables (map #(get-in % [:primary-table :name]) entities)]
    (dump.tables/dump-tables
     cassandra
     keyspace
     directory
     tables)))

(defn truncate-all-entity-tables
  "truncate all tables of an entity"
  [cassandra
   entity]
  (ddo [:let [keyspace (cass.session/keyspace cassandra)
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
         (stream/count-all-throw
          ::truncate-all-entity-tables))))

(defn load-new-record-s->entity
  "load a stream of *new* records to an entity

   if the records are not new records (i.e. some records with the same PK already
   exist) then all bets are off and you will get a mess"
  ([cassandra entity r-s]
   (load-new-record-s->entity cassandra entity {} {} r-s))
  ([cassandra entity cassandra-opts r-s]
   (load-new-record-s->entity cassandra entity {} cassandra-opts r-s))
  ([cassandra
    entity
    {notify-s :notify-s
     notify-cnt :notify-cnt
     concurrency :concurrency
     :as opts}
    cassandra-opts
    r-s]
   (ddo [:let [notify-s (or notify-s (dump.transit/log-notify-stream))
               notify-cnt (or notify-cnt 10000)
               concurrency (or concurrency 5)

               primary-table (get-in entity [:primary-table :name])
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

         total-cnt (->> r-s
                        (stream/map-concurrently
                         concurrency
                         (fn [r]
                           (swap! counter-a update-counter-fn)

                           (cass.m/change
                            cassandra
                            entity
                            nil
                            r
                            (merge
                             {::cass.t/skip-protect true}
                             cassandra-opts))))
                        (stream/count-all-throw
                         ::load-record-s->entity))]

     (when notify-s
       (stream/put! notify-s [primary-table total-cnt :drained])
       (stream/close! notify-s))

     (return total-cnt))))

(defn truncate-load-full-record-s->entity!!
  "load a stream of records to an entity *after* truncating any existing records
   from the db "
  ([cassandra entity r-s]
   (truncate-load-full-record-s->entity!! cassandra entity {} {} r-s))
  ([cassandra entity cassandra-opts r-s]
   (truncate-load-full-record-s->entity!! cassandra entity {} cassandra-opts r-s))
  ([cassandra entity opts cassandra-opts r-s]
   (ddo [;; truncating means we can avoid inserting any null columns
         ;; and avoid creating lots of tombstones
         _ (truncate-all-entity-tables
            cassandra
            entity)]
     (load-new-record-s->entity cassandra entity opts cassandra-opts r-s))))

(defn load-entity!!
  "load an entity from a dump of its primary table

   note - truncates the entity's tables before loading"
  [cassandra
   directory
   cassandra-opts
   entity]
  (ddo [:let [keyspace (cass.session/keyspace cassandra)
              table (-> entity :primary-table :name)]
        raw-s (dump.tables/transit-file->entity-record-s
               keyspace
               directory
               table)
        :let [r-s (->> raw-s
                       (stream/map
                        (fn [r]
                          (cass.cb/chain-callbacks
                           cassandra
                           entity
                           [:deserialize :after-load]
                           r
                           {}))))]]
    (truncate-load-full-record-s->entity!!
     cassandra
     entity
     {:notify-s (dump.transit/log-notify-stream)}
     cassandra-opts
     r-s)))

(defn load-entities!!
  "load a list of entities from dumps of their primary tables

   note - truncates all the entities tables before loading"
  [cassandra
   directory
   cassandra-opts
   entities]
  (->> entities
       (stream/->source)
       (stream/map-concurrently
        5
        #(load-entity!!
          cassandra
          directory
          cassandra-opts
          %))
       (stream/count-all-throw
        ::load-entities)))
