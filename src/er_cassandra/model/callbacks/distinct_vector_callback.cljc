(ns er-cassandra.model.callbacks.distinct-vector-callback
  (:require
   [plumbing.core :refer [assoc-when]]))

(defn create-distinct-vector-callback
  "replaces the value of column, if present, with
   a distinct vector of non-nil values - dups and nils
   are removed"
  [col]
  (fn [r]
    (let [v (get r col)]
      (assoc-when r col (when v
                          (vec
                           (filter some?
                                   (distinct v))))))))
