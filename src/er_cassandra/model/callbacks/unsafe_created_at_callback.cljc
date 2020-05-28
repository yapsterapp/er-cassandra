(ns er-cassandra.model.callbacks.unsafe-created-at-callback
  (:require
   #?@(:clj  [[clj-time.core :as t]
              [clj-time.coerce :as t.coerce]]
       :cljs [[cljs-time.core :as t]
              [cljs-time.coerce :as t.coerce]])
   #?(:clj [clj-uuid :as uuid])))

(defn ^:deprecated unsafe-created-at-callback
  "it's sort of kinda like a created-at, but it's not very safe,
   because there is no sane way of providing the guarantee

   use timestamp-timeuuid-callback/created-at-callback instead"
  ([] (unsafe-created-at-callback :created_at))
  ([col]
   (fn [r]
     (let [v (get r col)]
       (if v
         r
         (assoc r col (t.coerce/to-date (t/now))))))))

#?(:clj
   (defn ^:deprecated unsafe-created-at-timeuuid-callback
     "use timestamp-timeuuid-callback/created-at-timeuuid-callback instead"
     ([] (unsafe-created-at-timeuuid-callback :created_at_timeuuid))
     ([col]
      (fn [r]
        (let [v (get r col)]
          (if v
            r
            (assoc r col (uuid/v1))))))))

#?(:clj
   (defn ^:deprecated timestamp-from-timeuuid-callback
     "use use timestamp-timeuuid-callback/timestamp-from-timeuuid-callback instead"
     [timeuuid-col timestamp-col]
     (fn [r]
       (let [t-uuid (get r timeuuid-col)]
         (if (uuid/uuid? t-uuid)
           (assoc r timestamp-col (uuid/get-instant t-uuid))
           r)))))
