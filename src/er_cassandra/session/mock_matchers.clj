(ns er-cassandra.session.mock-matchers
  (:require
   [plumbing.core :refer :all]
   [taoensso.timbre :refer [trace debug info warn error]]
   [clojure.pprint :refer [pprint]]))


(defnk match-select
  [select
   where
   response
   :as params]
  (fn [statement]
    (when (and (= select (:select statement))
               (= where (:where statement)))
      response)))
