(ns er-cassandra.model.callbacks.default-value-callback
  (:require
   [clojure.string :as str]))

(defn create-default-value-callback
  [col default-val]
  (fn [r]
    (let [v (get r col)]
      (assoc r col (or v default-val)))))
