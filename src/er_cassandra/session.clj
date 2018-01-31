(ns er-cassandra.session)

(defprotocol Session
  (execute
    [this statement opts])
  (execute-buffered
    [this statement opts])
  (close [this]))

(defprotocol SpySession
  (spy-log [this])
  (reset-spy-log [this]))

(defprotocol KeyspaceProvider
  (keyspace [this]))
