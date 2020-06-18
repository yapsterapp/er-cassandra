(ns er-cassandra.schema.columns
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [qbits.hayt :as h]
   [qbits.hayt.dsl :as cql]
   [er-cassandra.migrations.schema-helpers :as cass.sch.help]
   [er-cassandra.session :as session]
   [er-cassandra.schema :as cass.sch]
   [cats.core :as monad :refer [return]]
   [prpr.promise :as prpr :refer [ddo]]))

(defn check-no-keyspace
  "checks that there is no keyspace in a keyword"
  [k]
  (let [ks (str k)
        has-period? (str/includes? ks ".")]
    (when has-period?
      (throw
       (ex-info "key has keyspace"
                {:k k})))
    true))

(defn alter-table-add-column
  [cassandra table column type]
  (ddo [r (session/execute
           cassandra
           (h/->raw
            (cql/alter-table
             table
             (cql/add-column column type)))
           {})]
    (return
     [::column-added {:table table
                      :column column
                      :type type}])))

(defn alter-table-drop-column
  [cassandra table column]
  (ddo [r (session/execute
           cassandra
           (h/->raw
            (cql/alter-table
             table
             (cql/drop-column column)))
           {})]
    (return
     [::column-dropped {:table table
                        :column column}])))

(defn add-column-if-not-exists
  [cassandra table column type]
  (ddo [{col-type :type
         :as col-md} (cass.sch/column-metadata
                      cassandra
                      table
                      column)
        type-kw (keyword type)]
    (cond
      (nil? col-md)
      (alter-table-add-column cassandra table column type-kw)

      (and (some? col-md)
           (= col-type (name type-kw)))
      (return
       [::column-exists {:table table
                         :column column
                         :type type-kw}])

      :else
      (prpr/error-pr
       [::column-exists-with-different-type
        {:table table
         :column column
         :col-type col-type
         :type type}]))))

(defn drop-column-if-exists
  [cassandra table column]
  (ddo [{col-type :type
         :as col-md} (cass.sch/column-metadata
                      cassandra
                      table
                      column)]
    (cond
      (nil? col-md)
      (return
       [::column-does-not-exist {:table table
                                 :column column}])

      :else
      (alter-table-drop-column cassandra table column))))


(defn rename-column
  [cassandra table old-column-name new-column-name]
  (ddo [r (session/execute
           cassandra
           (h/->raw
            (cql/alter-table
             table
             (cql/rename-column
              old-column-name
              new-column-name)))
           {})]
    (return
     [::column-renamed {:table table
                        :old-column-name old-column-name
                        :new-column-name new-column-name}])))

(defn rename-column-if-not-renamed
  [cassandra table old-column-name new-column-name]
  (ddo [{col-type :type
         :as new-col-md} (cass.sch/column-metadata
                          cassandra
                          table
                          new-column-name)]
    (cond
      (some? new-col-md)
      (return
       [::column-already-renamed
        {:table table
         :old-column-name old-column-name
         :new-column-name new-column-name}])

      :else
      (rename-column
       cassandra
       table
       old-column-name
       new-column-name))))

(defn add-selected-columns-to-view-if-not-exists
  [cassandra view selected-columns]
  (ddo [{view-columns :selected-columns
         :as view-def} (cass.sch.help/table-definition cassandra view)
        :let [cols-to-add (set/difference (set selected-columns) (set view-columns))
              recreate-view? (boolean (seq cols-to-add))
              new-view-def (when recreate-view?
                             (update view-def :selected-columns into cols-to-add))]
        _ (monad/when recreate-view?
            (session/execute cassandra (cass.sch.help/drop-view view-def) {}))
        _ (monad/when recreate-view?
            (session/execute cassandra (cass.sch.help/create-view new-view-def) {}))]
       (return (or new-view-def view-def))))

(defn drop-selected-columns-from-view-if-exists
  [cassandra view selected-columns]
  (ddo [{view-columns :selected-columns
         :as view-def} (cass.sch.help/table-definition cassandra view)
        :let [cols-to-drop (set/intersection (set view-columns) (set selected-columns))
              recreate-view? (boolean (seq cols-to-drop))
              new-view-def (when recreate-view?
                             (update
                              view-def
                              :selected-columns
                              (comp vec (partial remove cols-to-drop))))]
        _ (monad/when recreate-view?
            (session/execute cassandra (cass.sch.help/drop-view view-def) {}))
        _ (monad/when recreate-view?
            (session/execute cassandra (cass.sch.help/create-view new-view-def) {}))]
       (return (or new-view-def view-def))))
