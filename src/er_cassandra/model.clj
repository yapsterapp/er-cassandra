(ns er-cassandra.model
  (:require
   [potemkin :refer [import-vars]]
   [er-cassandra.model.types]
   [er-cassandra.model.select]
   [er-cassandra.model.upsert]
   [er-cassandra.model.delete]))

;; an opinionated Model which has one primary table,
;; zero or more secondary tables (from which the entire instance
;; can be retrieved), and zero or more lookup tables which
;; reference the primary table.
;;
;; the primary table and all the secondary tables of a model
;; *must* all have the exact same columns, since the intention
;; is that any primary or secondary table can be used for a
;; single-read lookup. secondary table records are implicitly
;; 0..1 <-> 1 with primary table records
;;
;; the primary-key of the primary table should be a uuid or
;; similar, which will never change for a given model instance.
;; this key will be used as the 'uber-key' for model instances
;; referenced from lookup tables
;;
;; lookup tables reference a value from a key which uniquely
;; identifies a single model instance to the uber-key for
;; the instance. lookup table records are 0..1 <-> 1 or
;; 0..* <-> 1 with primary table records, depending on the
;; type of the key column (list,set,map form 0..* <-> 1
;; relationships)
;;
;; list and set columns in the model can be used in lookup keys,
;; and they will be unrolled. maps columns can also be used, in
;; which case the map keys will be unrolled.
;; all values in the list/set/map-keyset must
;; uniquely identify the record to which they belong
;;
;; secondary indexes should be placed on the uber-key of all
;; secondary and lookup tables, to enable an easy delete of all
;; records for a given model instance

(import-vars

 [er-cassandra.model.types create-model defmodel]

 [er-cassandra.model.select
  select
  select-one
  select-one-instance
  ensure-one
  select-many
  select-many-instances
  select-many-cat]

 [er-cassandra.model.upsert upsert upsert-many]

 [er-cassandra.model.delete delete delete-many])
