(ns er-cassandra.model.util.timestamp)

(defn current-time-micros
  "return a long representing the current time since the epoch
   in microseconds, as used by cassandra for default TIMESTAMP
   values. the value only has millisecond resolution because JVM"
  []
  (* 1000
     (System/currentTimeMillis)))
