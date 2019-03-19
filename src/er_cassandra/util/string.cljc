(ns er-cassandra.util.string
  (:require
   [clojure.string :as str])
  #?(:clj
     (:import
      [java.text Normalizer Normalizer$Form])))

(defn normalize-string
  [s]
  (when s
    (-> (str/trim s)
        #?(:clj  (Normalizer/normalize Normalizer$Form/NFD)
           :cljs (.normalize "NFD"))
        (str/replace
         #?(:clj  #"\p{InCombiningDiacriticalMarks}+"
            :cljs #"[\u0300-\u036F\u1DC0-\u1DFF\u20D0-\u20FF]+")
         "")
        (str/lower-case))))

(defn string->re-pattern
  [s]
  (when s
    (-> s
        normalize-string
        re-pattern)))
