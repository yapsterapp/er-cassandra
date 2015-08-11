(ns er-cassandra.session
  (:require
   [plumbing.core :refer :all]
   [qbits.alia :as alia]
   [qbits.alia.manifold :as aliam]
   [manifold.deferred :as d]
   [qbits.hayt :as h]
   [clj-uuid :as uuid]
   [er-cassandra.key :refer [make-sequential extract-key-equality-clause]]))

(defprotocol Session
  (execute [this statement])
  (close [this]))
