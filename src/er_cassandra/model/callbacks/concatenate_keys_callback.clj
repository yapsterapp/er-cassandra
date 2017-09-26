(ns er-cassandra.model.callbacks.concatenate-keys-callback
  (:require
   [er-cassandra.concatenated-key :as ck]
   [clojure.string :as str]))

(defn concatenate-keys-callback
  "callback to synthesize a new key by concatenating other keys,
   with SQL NULL semantics"
  ([to-col key-cols]
   (concatenate-keys-callback to-col ck/default-separator key-cols))

  ([to-col separator key-cols]
   (fn [r]
     (let [key-col-vals (->> key-cols
                             (map #(get r %)))
           to-col-val (ck/concatenate-keys separator key-col-vals)]

       (assoc r to-col to-col-val)))))
