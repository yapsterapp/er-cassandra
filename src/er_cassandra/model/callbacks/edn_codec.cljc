(ns er-cassandra.model.callbacks.edn-codec
  (:require
   [taoensso.timbre #?(:clj :refer :cljs :refer-macros) [debug]]
   #?(:clj [clojure.edn :as edn]
      :cljs [cljs.reader :as edn])))

(defn edn-serialize-callback
  [col]
  (assert (keyword? col))
  (fn [r]
    (let [col? (contains? r col)
          v (get r col)]
      (cond
        ;; i would prefer not to set the value if the key
        ;; wasn't in the map, but it breaks a lot of tests
        ;;
        ;; UPDATE 20181018 mccraig - we gotta deal with the
        ;; test breakage 'cos this is causing :attrs and
        ;; :perms to get temporarily removed from :org_user
        ;; records during import with consequent perms failures
        (not col?) r
        (nil? v) (assoc r col nil)
        (and (string? v) (empty? v)) (assoc r col nil)
        (string? v) r
        :else (binding [*print-length* -1]
                (update r col pr-str))))))

(defn edn-deserialize-callback
  ([col] (edn-deserialize-callback col nil {}))
  ([col default-value] (edn-deserialize-callback col default-value {}))
  ([col default-value reader-opts]
   (assert (keyword? col))
   (fn [r]
     (let [col? (contains? r col)
           v (get r col)]
       ;;:completion_status [:started] nil
       (try
         (cond
           ;; i would prefer not to set the value if the key
           ;; wasn't in the map, but lots of test breakage results
           ;;
           ;; UPDATE 20181018 mccraig - we gotta deal with the
           ;; test breakage 'cos this is causing :attrs and
           ;; :perms to get temporarily removed from :org_user
           ;; records during import with consequent perms failures
           (not col?) r
           (nil? v) (assoc r col default-value)
           (not (string? v)) r
           :else (update r col #(edn/read-string reader-opts %)))
         (catch #?(:clj Exception :cljs Error) err
           (debug err (str "Failed to deserialize value:" v))
           (throw err)))))))
