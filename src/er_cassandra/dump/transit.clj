(ns er-cassandra.dump.transit
  (:require
   [cats.core :as monad :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [cognitect.transit :as transit]
   [clojure.java.io :as io]
   [prpr.promise :as pr :refer [ddo]]
   [prpr.stream :as stream]
   [manifold.deferred :as d]
   [taoensso.timbre :as timbre :refer [info]])
  (:import
   [com.cognitect.transit WriteHandler ReadHandler]
   [java.io EOFException]))

(def custom-transit-write-handlers
  {java.lang.Integer
   (reify WriteHandler
     (tag [_ _] "i32")
     (rep [_ i] (.toString i))
     (stringRep [this bi] (.rep this bi))
     (getVerboseHandler [_] nil))})

(def transit-write-handler-map
  (transit/write-handler-map
   custom-transit-write-handlers))

(def custom-transit-read-handlers
  {"i32" (reify ReadHandler
           (fromRep [_ o]
             (java.lang.Integer/parseInt ^String o)))})

(def transit-read-handler-map
  (transit/read-handler-map
   custom-transit-read-handlers))

(defn record-s->transit-file
  "write a stream of records to a file as transit"
  [f
   {:keys [stream-name
           notify-s
           notify-cnt]
    :as opts}
   r-s]
  (ddo [:let [out (-> f io/file io/output-stream)
              w (transit/writer out
                                :msgpack
                                {:handlers transit-write-handler-map})
              notify-cnt (or notify-cnt 10000)
              counter-a (atom 0)

              update-counter-fn (fn [cnt]
                                  (let [nc (inc cnt)]
                                    (when (and
                                           notify-s
                                           (= 0 (mod nc notify-cnt)))
                                      (stream/try-put!
                                       notify-s
                                       [stream-name nc]
                                       0))
                                    nc))

              no-buffer-s (stream/stream)
              _ (stream/connect r-s no-buffer-s)]

        total-cnt (->> no-buffer-s
                       (stream/realize-each)
                       (stream/map
                        (fn [r]
                          (swap! counter-a update-counter-fn)
                          (transit/write w r)))
                       (stream/count-all-throw
                        ::record-s->transit-file))]

    (.close out)
    (when notify-s
      (stream/put! notify-s [stream-name total-cnt :drained])
      (stream/close! notify-s))
    (return total-cnt)))

(defn transit-file->record-s
  "uses a thread to read EDN from a file and return a stream of records
   from the file"
  [f]
  (let [s (stream/stream 5000)]
    (d/finally
      (pr/catch-error-log
       (str "file->record-s: " (pr-str f))
       (d/future
         (let [in (-> f
                      io/file
                      io/input-stream)
               r (transit/reader in
                                 :msgpack
                                 {:handlers transit-read-handler-map})]

           (pr/catch
               (fn [x]
                 ;; transit throws a RuntimeException on EOF!
                 (.close in)
                 (pr/success-pr true))

               (d/loop [v (transit/read r)]
                 (ddo [_ (stream/put! s v)]
                   (d/recur (transit/read r))))))))
      (fn [] (stream/close! s)))

    (return deferred-context s)))

(defn log-notify-stream
  "returns a notify-s which expects [stream-name count drained?] records
   and logs them with the supplied level"
  ([] (log-notify-stream :info))
  ([level]
   (let [s (stream/stream 100)]
     (stream/consume
      (fn [[stream-name count drained?]]
        (if drained?
          (timbre/log level stream-name count "FINISHED")
          (timbre/log level stream-name count))
        )
      s)
     s)))
