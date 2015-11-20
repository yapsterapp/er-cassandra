(ns er-cassandra.model.error)

(defn field-error-log-entry
  "create a :field-error log entry"
  ([field error-key error-msg]
   (field-error-log-entry field error-key error-msg nil))
  ([field error-key error-msg value]
   [:field-error
    [field (merge {:field field
                   :error-key error-key
                   :error-msg error-msg}
                  (when value {:value value}))]]))

(defn general-error-log-entry
  "create a :general-error log entry"
  ([error-key error-msg]
   (general-error-log-entry error-key error-msg nil))
  ([error-key error-msg value]
   [:general-error (merge {:error-key error-key
                           :error-msg error-msg}
                          (when value {:value value}))]))
