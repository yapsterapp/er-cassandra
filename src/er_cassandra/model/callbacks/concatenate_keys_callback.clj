(ns er-cassandra.model.callbacks.concatenate-keys-callback
  (:require
   [clojure.string :as str]))

(def default-separator "/")

(defn concatenate-keys
  "concatenate some key values with SQL NULL semantics - any nil
   key-col-val results in a nil result"
  ([key-col-vals]
   (concatenate-keys default-separator key-col-vals))

  ([separator key-col-vals]
   (when (every? identity key-col-vals)
     (->> key-col-vals
          (map str)
          (str/join separator)))))

(defn concatenate-keys-callback
  "callback to synthesize a new key by concatenating other keys,
   with SQL NULL semantics"
  ([to-col key-cols]
   (concatenate-keys-callback to-col default-separator key-cols))

  ([to-col separator key-cols]
   (fn [r]
     (let [key-col-vals (->> key-cols
                             (map #(get r %)))
           to-col-val (concatenate-keys separator key-col-vals)]

       (assoc r to-col to-col-val)))))
