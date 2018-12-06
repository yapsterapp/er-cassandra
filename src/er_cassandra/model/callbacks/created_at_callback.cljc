(ns er-cassandra.model.callbacks.created-at-callback
  (:require
   #?(:clj [clj-uuid :as uuid])))

(defn created-at-callback
  "a callback to set a :created_at field from a V1 uuid field"
  ([] (created-at-callback :id :created_at))
  ([id-col created-at-col]
   (fn [r]
     (if (nil? (get r created-at-col))
       (assoc r created-at-col #?(:clj (uuid/get-instant (get r id-col))
                                  :cljs (js/Date.)))
       r))))
