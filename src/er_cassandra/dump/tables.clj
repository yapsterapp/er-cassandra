(ns er-cassandra.dump.tables
  (:require
   [cats.core :as monad :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [cognitect.transit :as transit]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [er-cassandra.record :as cass.r]
   [er-cassandra.session :as cass.session]
   [er-cassandra.schema :as cass.schema]
   [prpr.promise :as pr :refer [ddo]]
   [prpr.stream :as pr.st]
   [manifold.deferred :as d]
   [manifold.stream :as stream]
   [qbits.hayt :as h]
   [taoensso.timbre :as timbre :refer [info]]
   [taoensso.timbre :refer [warn]])
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

(defn keyspace-table-name
  [keyspace table]
  (keyword
   (str
    (name keyspace)
    "."
    (name table))))

(defn counter-table?
  "returns true if the given table is a counter table"
  [cassandra keyspace table]
  (ddo [{tm-flags :flags
         :as tm} (cass.schema/table-metadata
                  cassandra
                  keyspace
                  table)]
    (contains? tm-flags "counter")))

(defn key-counter-columns
  "returns [key-cols counter-col] for a counter table,
   nil otherwise"
  [cassandra keyspace table]
  (ddo [cols (cass.schema/table-columns
              cassandra
              keyspace
              table)
        :let [counter-col? #(= "counter" (:type %))
              [counter-col] (filter counter-col? cols)
              key-cols (remove counter-col? cols)]]
    (return
     (if (some? counter-col)
       [(->> key-cols
             (map :column_name)
             (mapv keyword))
        (-> counter-col
            :column_name
            keyword)]))))

(defn keyspace-table-name-s
  "return a stream of all the table names in a keyspace which
   are not in the skip set"
  [cassandra keyspace skip]
  (ddo [table-s (cass.session/execute-buffered
                 cassandra
                 (str
                  "select * from system_schema.tables where keyspace_name='"
                  (name keyspace)
                  "'")
                 {})

        :let [skip-set (->> skip (map keyword) set)]]
    (->> table-s
         (stream/map :table_name)
         (stream/map keyword)
         (stream/filter #(not (skip-set %)))
         return)))

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

(defn table->record-s
  "return a stream of records from a table"
  [cassandra
   keyspace
   table]
  (cass.r/select-buffered
   cassandra
   (keyspace-table-name keyspace table)))

(defn remove-nil-values
  [r]
  (->> r
       (filter (fn [[k v]] (some? v)))
       (into {})))

(defn dump-record-s->transit
  "write a stream of records to a file as EDN"
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
                          (transit/write w (remove-nil-values r))))
                       (pr.st/count-all-throw
                        ::dump-record-s->transit))]

    (.close out)
    (when notify-s
      (stream/put! notify-s [stream-name total-cnt :drained])
      (stream/close! notify-s))
    (return total-cnt)))

(defn dump-table
  [cassandra
   keyspace
   directory
   table]
  (ddo [:let [table (keyword table)
              directory (io/file directory)
              f (io/file directory (str (name table) ".transit"))]
        r-s (table->record-s
             cassandra
             keyspace
             table)]
    (dump-record-s->transit
     f
     {:stream-name table
      :notify-s (log-notify-stream)}
     r-s)))

(defn dump-tables
  [cassandra
   keyspace
   directory
   tables]
  (ddo [:let [table-s (stream/->source tables)]
        table-cnt (->> table-s
                       (stream/buffer 5)
                       (stream/map
                        (fn [t]
                          (ddo [r-s (table->record-s
                                     cassandra
                                     keyspace
                                     t)]
                            (dump-record-s->transit
                             (io/file directory
                                      (str (name t) ".transit"))
                             {:stream-name (name t)
                              :notify-s (log-notify-stream)}
                             r-s))))
                       (stream/realize-each)
                       (pr.st/count-all-throw
                        ::dump-tables))]
    (info "dump-tables dumped" table-cnt "tables - FINISHED")
    (return table-cnt)))

(defn dump-all-keyspace-tables
  "dump cassandra tables from a keyspace to EDN files in a directory"
  [cassandra
   keyspace
   directory
   skip]
  (ddo [table-s (keyspace-table-name-s cassandra keyspace skip)]
    (dump-tables
     cassandra
     keyspace
     directory
     table-s)))

(defn file->record-s
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

(defn insert-normal-record
  [cassandra
   keyspace
   table
   record]
  (cass.r/insert
   cassandra
   table
   (remove-nil-values record)
   {:prepare? true
    :consistency :any}))

(defn update-counter-record
  "counter tables require an update statment... which requires
   an assumption that the counter table has been truncated"
  [cassandra
   keyspace
   table
   counter-key-cols
   counter-col
   record]
  ;; (warn "update-counter-record"
  ;;       {:keyspace keyspace
  ;;        :table table
  ;;        :counter-key-cols counter-key-cols
  ;;        :counter-col counter-col
  ;;        :record record})
  (let [counter-val (get record counter-col)]
    (if (and (some? counter-val)
             (> counter-val 0))
      (cass.r/update
       cassandra
       (keyspace-table-name keyspace table)
       counter-key-cols
       (assoc
        record
        counter-col
        [:+ counter-val])
       {;; :prepare? has a bug for [:+] stmts
        ;; :prepare? true

        ;; :any not supported for prepared counter tables
        ;; :consistency :any
        })
      (pr/success-pr nil))))

(defn load-record-s->table
  "load a stream of records to a table"
  [cassandra
   keyspace
   table
   {notify-s :notify-s
    notify-cnt :notify-cnt
    :as opts}
   r-s]
  (ddo [:let [notify-cnt (or notify-cnt 10000)
              counter-a (atom 0)

              update-counter-fn (fn [cnt]
                                  (let [nc (inc cnt)]
                                    (when (and
                                           notify-s
                                           (= 0 (mod nc notify-cnt)))
                                      (stream/try-put!
                                       notify-s
                                       [table nc]
                                       0))
                                    nc))]

        [counter-key-cols
         counter-col
         :as counter-table] (key-counter-columns
                             cassandra
                             keyspace
                             table)

        ;; truncating means we can avoid inserting any null columns
        ;; and avoid creating lots of tombstones
        _ (cass.session/execute
           cassandra
           (h/truncate
            (keyspace-table-name keyspace table)) {})

        total-cnt (->> r-s
                       (stream/buffer 50)
                       (stream/map
                        (fn [r]
                          (swap! counter-a update-counter-fn)

                          (if (some? counter-col)
                            (update-counter-record
                             cassandra
                             keyspace
                             table
                             counter-key-cols
                             counter-col
                             r)
                            (insert-normal-record
                             cassandra
                             keyspace
                             table
                             r))))
                       (stream/realize-each)
                       (pr.st/count-all-throw
                        ::load-record-s->table))]

    (when notify-s
      (stream/put! notify-s [table total-cnt :drained])
      (stream/close! notify-s))

    (return total-cnt)))

(defn transit-file->record-s
  [keyspace
   directory
   table]
  (ddo [:let [table (keyword table)
              directory (-> directory io/file)
              f (io/file directory (str (name table) ".transit"))]]
    (file->record-s f)))

(defn load-table
  "load a single table"
  [cassandra
   keyspace
   directory
   table]
  (ddo [r-s (transit-file->record-s keyspace directory table)]
    (load-record-s->table
     cassandra
     keyspace
     table
     {:counter-table table
      :notify-s (log-notify-stream)}
     r-s)))

(defn load-tables
  [cassandra
   keyspace
   directory
   tables]
  (ddo [:let [table-s (stream/->source tables)]
        table-cnt (->> table-s
                       (stream/buffer 3)
                       (stream/map
                        (fn [[t-n f]]
                          (ddo [r-s (file->record-s f)]
                            (load-record-s->table
                             cassandra
                             keyspace
                             t-n
                             {:notify-s (log-notify-stream)}
                             r-s))))
                       (stream/realize-each)
                       (pr.st/count-all-throw
                        ::load-table))]
    (info "load-tables loaded " table-cnt "tables - FINISHED")
    (return table-cnt)))

(defn load-all-tables-from-directory
  "load tables from EDN files in a directory to a cassandra keyspace"
  [cassandra
   keyspace
   directory
   skip]
  (ddo [:let [skip-set (->> skip (map keyword) set)
              file-s (-> directory
                         io/file
                         .listFiles
                         seq
                         stream/->source)]
        table-s (->> file-s
                       (stream/map
                        (fn [f]
                          (let [[_ t-n] (re-matches #"^([^\.]+)(?:\..*)?$" (.getName f))]
                            [(keyword t-n) f])))
                       (stream/filter
                        (fn [[t-n f]]
                          (not (skip-set t-n)))))]
    (load-tables
     cassandra
     keyspace
     directory
     table-s)))
