(ns er-cassandra.util.stream
  (:require
   [manifold.deferred :as d]
   [manifold.stream :as s]))

(defn catch-value
  "given a Deferred value catch errors and return Deferred<Throwable>
   or if not in the error-state, just return Deferred<value>"
  [dv]
  (-> dv
      (d/catch (fn [v] v))))

(defn keep-error
  "reducer fn which keeps the first Throwable encountered,
   otherwise the last value encountered"
  [curr nxt]
  (if (instance? Throwable curr)
    curr
    nxt))

(defn throw-throwable
  "if the Deferred<value> is a Throwable, throw it"
  [dv]
  (-> dv
      (d/chain (fn [v]
                 (if (instance? Throwable v)
                   (throw v)
                   v)))))

(defn keep-stream-error
  "given a stream of Deferred<value|Throwable> reduce it to the first
   Throwable or (f final-value). the default f just returns nil"
  ([strm] (keep-stream-error strm (constantly nil)))
  ([strm f]
   (as-> strm %
     (s/map catch-value %)
     (s/realize-each %)
     (s/reduce keep-error nil %)
     (throw-throwable %)
     (d/chain % f))))
