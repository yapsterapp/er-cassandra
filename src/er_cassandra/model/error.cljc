(ns er-cassandra.model.error
  #?(:cljs
     (:require-macros [plumbing.core :refer [defnk]]))
  (:require
   #?(:clj [plumbing.core :refer :all]
      :cljs [plumbing.core :refer [assoc-when]])
   #?(:clj [clojure.core.match :refer [match]]
      :cljs [cljs.core.match :refer-macros [match]])
   [schema.core :as s #?@(:cljs [:include-macros true])]))

;; some functions for creating and collecting a log
;; of errors relating to an entity

;; general supporting information around an error
(def ErrorDescriptionSchema
  {(s/optional-key :type) s/Keyword
   :tag s/Keyword
   :message s/Str
   s/Keyword s/Any})

;; a tagged-variant description of an error
(def ErrorSchema
  [(s/one s/Keyword "error-tag")
   (s/one ErrorDescriptionSchema "description")])

(def EntityErrorDescriptionSchema
  (-> ErrorDescriptionSchema
      (dissoc (s/optional-key :type))
      (assoc :tpe (s/eq :entity)
             :primary-table s/Keyword
             :uber-key-value [s/Any])))

(def KeyErrorDescriptionSchema
  (-> EntityErrorDescriptionSchema
      (assoc :type (s/eq :key)
             :key [s/Keyword]
             :key-value [s/Any])))

;; errors tied to fields of submission
;; (sub-type of ErrorDescriptionSchema)
(def FieldErrorDescriptionSchema
  (-> EntityErrorDescriptionSchema
      (assoc :type (s/eq :field)
             :field s/Keyword
             :field-value s/Any)))

;; a log of errors, such as may be collected by a Writer
(defn ErrorLogSchema
  [ErrorDescriptionSchema])

(defnk entity-error-log-entry
  [error-tag
   message
   primary-table
   uber-key-value
   {other nil}]
  [error-tag
   (merge
    other
    {:tag error-tag
     :message message
     :type :entity
     :primary-table primary-table
     :uber-key-value uber-key-value})])

(defnk key-error-log-entry
  [error-tag
   message
   primary-table
   uber-key-value
   key
   key-value
   {other nil}]
  [error-tag
   (merge
    other
    {:tag error-tag
     :message message
     :type :key
     :primary-table primary-table
     :uber-key-value uber-key-value
     :key key
     :key-value key-value})])

(defnk filter-key-error-log-entries
  [error-tag primary-table key log]
  (->> log
       (filter
        (fn [[_ {t :tag
                 pt :primary-table
                 k :key}]]
          (and (= t error-tag)
               (= pt primary-table)
               (= k key))))))

(defnk field-error-log-entry
  "helper fn to create a :field-error log entry"
  [error-tag
   message
   primary-table
   uber-key-value
   field
   field-value
   {other nil}]
  [error-tag
   (merge
    other
    {:tag error-tag
     :message message
     :type :field
     :primary-table primary-table
     :uber-key-value uber-key-value
     :field field
     :value field-value})])

(defnk general-error-log-entry
  "helper fn to create a general error-log entry"
  [error-tag
   message
   {other nil}]
  [error-tag
   (merge
    other
    {:tag error-tag
     :message message})])

;; indexed errors
(def SubmissionErrorsSchema
  {:field-errors {s/Keyword [FieldErrorDescriptionSchema]}
   :key-errors {[s/Keyword] [KeyErrorDescriptionSchema]}
   :entity-errors [EntityErrorDescriptionSchema]
   :general-errors [ErrorSchema]})

(defn collect-error-log
  "collect an error-log into SubmissionErrors"
  [error-log]
  (reduce (fn [{field-errors :field-errors
                key-errors :key-errors
                entity-errors :entity-errors
                general-errors :general-errors
                :as ses}
               [e-tag {e-type :type
                       e-tag :tag
                       :as e-descr}]]
            (case e-type
              :field
              (let [f (:field e-descr)]
                (assoc-in ses
                          [:field-errors (:field e-descr)]
                          ((fnil conj []) (get field-errors f) e-descr)))

              :key
              (let [k (:key e-descr)]
                (assoc-in ses
                          [:key-errors (:key e-descr)]
                          ((fnil conj []) (get key-errors k) e-descr)))

              :entity
              (assoc-in ses
                        [:entity-errors]
                        ((fnil conj []) entity-errors e-descr))

              (assoc-in ses
                        [:general-errors]
                        ((fnil conj []) general-errors e-descr))))
          {:field-errors {}
           :key-errors {}
           :entity-errors []
           :general-errors []}
          error-log))
