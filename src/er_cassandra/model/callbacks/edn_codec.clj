(ns er-cassandra.model.callbacks.edn-codec
  (:require
   [clojure.edn :as edn]))

(defn edn-serialize-callback
  [col]
  (assert (keyword? col))
  (fn [r]
    (let [v (get r col)]
      (cond
        (nil? v) (assoc r col nil)
        (and (string? v) (empty? v)) (assoc r col nil)
        (string? v) r
        :else (update r col pr-str)))))

(defn edn-deserialize-callback
  ([col] (edn-deserialize-callback col nil))
  ([col default-value]
   (assert (keyword? col))
   (fn [r]
     (let [v (get r col)]
       (cond
         (nil? v) (assoc r col default-value)
         (not (string? v)) r
         :else (update r col edn/read-string))))))
