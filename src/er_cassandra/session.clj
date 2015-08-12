(ns er-cassandra.session)

(defprotocol Session
  (execute [this statement])
  (close [this]))
