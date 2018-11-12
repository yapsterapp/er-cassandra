(ns er-cassandra.schema.columns
  (:require
   [clojure.string :as str]
   [qbits.hayt :as h]
   [qbits.hayt.dsl :as cql]
   [er-cassandra.session :as session]
   [er-cassandra.schema :as cass.sch]
   [cats.core :refer [return]]
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
                      column)]
    (cond
      (nil? col-md)
      (alter-table-add-column cassandra table column type)

      (and (some? col-md)
           (= col-type (name type)))
      (return
       [::column-exists {:table table
                         :column column
                         :type type}])

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
