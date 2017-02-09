(ns er-cassandra.model.util.timestamp)

(defn current-time-micros
  "return a long representing the current time since the epoch
   in microseconds, as used by cassandra for default TIMESTAMP
   values. the value only has millisecond resolution because JVM"
  []
  (* 1000
     (System/currentTimeMillis)))

(defn default-timestamp-opt
  "generates a timestamp for USING TIMESTAMP based on the current clock"
  ([] (default-timestamp-opt nil))
  ([opts]
   (update-in opts
              [:using :timestamp]
              (fn [v]
                (or v
                    (current-time-micros))))))

(defn past-timestamp-opt
  "generates a USING TIMESTAMP at least 10 ms ago, useful for unit test fixtures"
  ([] (past-timestamp-opt nil))
  ([opts]
   (update-in opts
              [:using :timestamp]
              (fn [v]
                (or v
                    (-
                     (current-time-micros)
                     10000))))))
