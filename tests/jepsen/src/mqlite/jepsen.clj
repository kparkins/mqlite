(ns mqlite.jepsen
  "Jepsen workloads for mqlite's embedded client API."
  (:refer-clojure :exclude [test])
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.control :as control]
            [jepsen.core :as jepsen]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.history :as h]
            [jepsen.independent :as independent]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]
            [jepsen.store :as store]
            [jepsen.tests.linearizable-register :as register])
  (:import (java.lang Process ProcessBuilder ProcessBuilder$Redirect)
           (java.net InetSocketAddress ServerSocket Socket)
           (java.util.concurrent TimeUnit)))

(def default-options
  {:workload "all"
   :nemesis "restart"
   :time-limit 15
   :rate 30.0
   :concurrency 8
   :nemesis-interval 3.0
   :recovery-time 1.0
   :host "127.0.0.1"
   :store-dir "store"
   :start-timeout-ms 10000})

(declare adapter-command! ensure-workload-schema!)

(def count-doc-count 64)
(def count-final-rounds 8)
(def delete-doc-count 64)
(def drop-index-doc-count 64)
(def drop-index-final-rounds 4)
(def compound-doc-count 64)
(def compound-a-count 4)
(def compound-b-count 4)
(def compound-final-rounds 3)
(def index-build-doc-count 96)
(def index-build-final-rounds 4)
(def index-build-seed-count 64)
(def multikey-doc-count 64)
(def multikey-final-rounds 4)
(def claim-job-count 64)
(def long-scan-doc-count 32)
(def long-scan-final-rounds 4)
(def batch-prefix-width 5)
(def batch-prefix-ok-count 2)
(def batch-prefix-error-index 2)
(def namespace-b-offset 1000000)
(def secondary-key-count 8)
(def secondary-doc-count 64)
(def secondary-final-rounds 4)
(def unique-key-count 32)

(defn usage
  "Returns command-line help text."
  []
  (str/join
    "\n"
    ["Usage: clojure -M:test [options]"
     ""
     "Options:"
     (str "  --workload all|register|set|unique-index|secondary-index|"
          "read-your-writes|delete-set|namespace-isolation|"
          "count-consistency|index-build|drop-index|compound-index|"
          "multikey-index|find-and-modify-claim|long-scan-snapshot|"
          "write-batch-prefix"
          " Workload to run (default: all)")
     "  --nemesis restart|none           Fault model (default: restart)"
     "  --time-limit SECONDS             Runtime per workload (default: 15)"
     "  --rate OPS_PER_SECOND            Approximate global op rate (default: 30)"
     "  --concurrency N                  Jepsen client threads (default: 8)"
     "  --nemesis-interval SECONDS       Restart cadence (default: 3)"
     "  --recovery-time SECONDS          Wait after final restart (default: 1)"
     "  --repo-root PATH                 Repository root"
     "  --binary PATH                    Built mqlite Jepsen adapter binary"
     "  --port PORT                      Adapter port (default: auto)"
     "  --db-path PATH                   Database path (default: target/jepsen)"
     "  --store-dir PATH                 Jepsen store dir (default: store)"
     "  --help                           Show this help"]))

(defn parse-int
  "Parses a base-10 integer CLI value."
  [s]
  (Integer/parseInt s))

(defn parse-f64
  "Parses a floating-point CLI value."
  [s]
  (Double/parseDouble s))

(defn require-value
  "Returns the next CLI value or throws a useful error."
  [flag value]
  (when-not value
    (throw (ex-info (str flag " requires a value") {:flag flag})))
  value)

(defn parse-args
  "Parses mqlite Jepsen command-line options."
  [args]
  (loop [opts default-options
         args (seq args)]
    (if-not args
      opts
      (let [[flag value & more] args]
        (case flag
          "--help" (assoc opts :help? true)
          "-h" (assoc opts :help? true)
          "--workload" (recur (assoc opts :workload (require-value flag value))
                              more)
          "--nemesis" (recur (assoc opts :nemesis (require-value flag value))
                             more)
          "--time-limit" (recur (assoc opts :time-limit
                                       (parse-f64
                                         (require-value flag value)))
                                more)
          "--rate" (recur (assoc opts :rate
                                  (parse-f64
                                    (require-value flag value)))
                           more)
          "--concurrency" (recur (assoc opts :concurrency
                                         (parse-int
                                           (require-value flag value)))
                                  more)
          "--nemesis-interval" (recur (assoc opts :nemesis-interval
                                             (parse-f64
                                               (require-value flag value)))
                                      more)
          "--recovery-time" (recur (assoc opts :recovery-time
                                          (parse-f64
                                            (require-value flag value)))
                                   more)
          "--repo-root" (recur (assoc opts :repo-root
                                      (require-value flag value))
                               more)
          "--binary" (recur (assoc opts :binary (require-value flag value))
                            more)
          "--port" (recur (assoc opts :port
                                  (parse-int (require-value flag value)))
                           more)
          "--db-path" (recur (assoc opts :db-path
                                     (require-value flag value))
                              more)
          "--store-dir" (recur (assoc opts :store-dir
                                       (require-value flag value))
                                more)
          (throw (ex-info (str "Unknown option " flag) {:flag flag})))))))

(defn canonical-path
  "Returns a canonical filesystem path."
  [path]
  (.getCanonicalPath (io/file path)))

(defn repo-root
  "Returns the configured repository root."
  [opts]
  (canonical-path (or (:repo-root opts) "../..")))

(defn default-binary
  "Returns the default embedded Jepsen adapter binary path."
  [opts]
  (str (repo-root opts) "/target/debug/mqlite_jepsen_adapter"))

(defn free-port
  "Asks the OS for a currently free TCP port."
  []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn workload-db-path
  "Returns the database path for a workload."
  [opts workload]
  (or (:db-path opts)
      (str (repo-root opts)
           "/target/jepsen/"
           "mqlite-jepsen-"
           workload
           "-"
           (System/currentTimeMillis)
           ".mqlite")))

(defn delete-file-if-present!
  "Deletes a file if it already exists."
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (io/delete-file file))))

(defn reset-db-files!
  "Deletes mqlite database artifacts for a fresh Jepsen run."
  [db-path log-path]
  (doseq [path [db-path (str db-path "-journal") log-path]]
    (delete-file-if-present! path)))

(defn ensure-parent!
  "Creates a path's parent directory when needed."
  [path]
  (when-let [parent (.getParentFile (io/file path))]
    (.mkdirs parent)))

(defn live-process?
  "Returns true if the process atom contains a live process."
  [process-atom]
  (boolean
    (when-let [^Process process @process-atom]
      (.isAlive process))))

(defn current-port
  "Returns the numeric port from a fixed value or mutable port holder."
  [port]
  (if (instance? clojure.lang.IDeref port)
    @port
    port))

(defn adapter-ready?
  "Returns true when host:port responds to the adapter ping command."
  [host port]
  (try
    (= :ok (:tag (adapter-command! host port "ping")))
    (catch Exception _
      false)))

(defn wait-for-adapter!
  "Waits until host:port responds as the mqlite adapter."
  [host port timeout-ms process-atom]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (adapter-ready? host port)
        true
        (do
          (when-let [^Process process @process-atom]
            (when-not (.isAlive process)
              (throw (ex-info "mqlite adapter exited before becoming ready"
                              {:exit (.exitValue process)}))))
          (when (< deadline (System/currentTimeMillis))
            (throw (ex-info "mqlite adapter did not become ready in time"
                            {:host host :port port :timeout-ms timeout-ms})))
          (Thread/sleep 100)
          (recur))))))

(defn stop-started-process!
  "Stops a process started during adapter launch."
  [^Process process]
  (when (.isAlive process)
    (.destroy process)
    (when-not (.waitFor process 5 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 TimeUnit/SECONDS))))

(defn launch-server-once!
  "Starts one adapter process and returns either a process or launch error."
  [{:keys [binary db-path host port process log-path start-timeout-ms]}]
  (let [actual-port (current-port port)
        log-file (io/file log-path)
        builder (ProcessBuilder.
                  ^java.util.List
                  [binary "--host" host "--port" (str actual-port)
                   "--db-path" db-path])
        _ (.redirectErrorStream builder true)
        _ (.redirectOutput builder
                            (ProcessBuilder$Redirect/appendTo log-file))
        started (.start builder)]
    (reset! process started)
    (try
      (wait-for-adapter! host actual-port start-timeout-ms process)
      {:process started}
      (catch Exception error
        (stop-started-process! started)
        (reset! process nil)
        {:error error}))))

(defn launch-server!
  "Starts the embedded mqlite Jepsen adapter."
  [{:keys [binary db-path fixed-port? port process] :as db}]
  (locking process
    (if (live-process? process)
      @process
      (loop [attempt 0]
        (when-not (.exists (io/file binary))
          (throw (ex-info "mqlite adapter binary does not exist"
                          {:binary binary})))
        (ensure-parent! db-path)
        (ensure-parent! (:log-path db))
        (let [{:keys [process error]} (launch-server-once! db)]
          (cond
            process process
            (and (not fixed-port?) (< attempt 7)) (do
                                                    (reset! port (free-port))
                                                    (recur (inc attempt)))
            :else (throw error)))))))

(defn stop-process!
  "Stops the mqlite Jepsen adapter gracefully, then forcefully if needed."
  [process-atom]
  (locking process-atom
    (when-let [^Process process @process-atom]
      (when (.isAlive process)
        (.destroy process)
        (when-not (.waitFor process 5 TimeUnit/SECONDS)
          (.destroyForcibly process)
          (.waitFor process 5 TimeUnit/SECONDS)))
      (reset! process-atom nil))))

(defn kill-process!
  "Kills the mqlite Jepsen adapter forcefully."
  [process-atom]
  (locking process-atom
    (when-let [^Process process @process-atom]
      (when (.isAlive process)
        (.destroyForcibly process)
        (.waitFor process 5 TimeUnit/SECONDS))
      (reset! process-atom nil))))

(defrecord MqliteDB [binary db-path fixed-port? host port process log-path
                     start-timeout-ms workload]
  db/DB
  (setup! [this _test _node]
    (reset-db-files! db-path log-path)
    (launch-server! this)
    (ensure-workload-schema! this)
    this)

  (teardown! [_this _test _node]
    (stop-process! process))

  db/Kill
  (kill! [_this _test _node]
    (kill-process! process))

  (start! [this _test _node]
    (launch-server! this)))

(defn mqlite-db
  "Builds a Jepsen DB wrapper for a local embedded adapter process."
  [opts workload]
  (let [path (canonical-path (workload-db-path opts workload))
        log-path (str path ".log")]
    (map->MqliteDB
      {:binary (canonical-path (or (:binary opts) (default-binary opts)))
       :db-path path
       :fixed-port? (some? (:port opts))
       :host (:host opts)
       :port (atom (or (:port opts) (free-port)))
       :process (atom nil)
       :log-path log-path
       :start-timeout-ms (:start-timeout-ms opts)
       :workload workload})))

(defrecord RestartNemesis [db]
  nemesis/Nemesis
  (setup! [this _test] this)

  (invoke! [this test op]
    (let [node (first (:nodes test))]
      (case (:f op)
        :kill (do
                (db/kill! db test node)
                (assoc op :type :info :value :killed))
        :start (do
                 (db/start! db test node)
                 (assoc op :type :info :value :started))
        (assoc op :type :info :value :noop))))

  (teardown! [this test]
    (db/start! db test (first (:nodes test)))
    this)

  nemesis/Reflection
  (fs [_this] #{:kill :start}))

(defn workload-nemesis
  "Returns the configured Jepsen nemesis."
  [opts db]
  (case (:nemesis opts)
    "none" nemesis/noop
    "restart" (->RestartNemesis db)
    (throw (ex-info "Unsupported nemesis" {:nemesis (:nemesis opts)}))))

(defn restart-generator
  "Returns a nemesis generator for process restarts."
  [opts]
  (case (:nemesis opts)
    "none" nil
    "restart" (->> (cycle [{:f :kill} {:f :start}])
                   (gen/stagger (:nemesis-interval opts)))
    (throw (ex-info "Unsupported nemesis" {:nemesis (:nemesis opts)}))))

(defn with-restart-nemesis
  "Adds the restart nemesis to a client generator when configured."
  [opts client-gen]
  (if-let [nemesis-gen (restart-generator opts)]
    (gen/nemesis nemesis-gen client-gen)
    client-gen))

(defn recovering-generator
  "Runs a workload, ensures the server is up, then optionally runs final reads."
  ([opts client-gen]
   (recovering-generator opts client-gen nil))
  ([opts client-gen final-gen]
   (let [active (->> client-gen
                     (gen/stagger (/ 1.0 (:rate opts)))
                     (with-restart-nemesis opts)
                     (gen/time-limit (:time-limit opts)))
         recovery [(gen/nemesis {:f :start})
                   (gen/sleep (:recovery-time opts))]]
     (apply gen/phases (cond-> [active]
                         (= "restart" (:nemesis opts)) (into recovery)
                         final-gen (conj final-gen))))))

(defrecord ClientOnlyChecker [checker]
  checker/Checker
  (check [_this test history opts]
    (checker/check checker test (h/client-ops history) opts)))

(defn client-only-checker
  "Wraps a checker so it only sees client operations."
  [checker]
  (->ClientOnlyChecker checker))

(def operation-timeout-ms 2000)

(defn op-info
  "Marks an operation as indeterminate after a client-side exception."
  [op error]
  (assoc op :type :info :error (str (class error) ": " (.getMessage error))))

(defn connect-socket!
  "Opens a TCP socket to the local mqlite Jepsen adapter."
  [host port]
  (let [socket (Socket.)]
    (.connect socket (InetSocketAddress. host port) operation-timeout-ms)
    (.setSoTimeout socket operation-timeout-ms)
    socket))

(defn parse-long-token
  "Parses an adapter integer token."
  [token]
  (Long/parseLong token))

(defn parse-pair-token
  "Parses an adapter id:value token."
  [token]
  (let [[id value] (str/split token #":" 2)]
    [(parse-long-token id) (parse-long-token value)]))

(defn parse-triple-token
  "Parses an adapter id:first:second token."
  [token]
  (let [[id first second] (str/split token #":" 3)]
    [(parse-long-token id) (parse-long-token first) (parse-long-token second)]))

(defn parse-response
  "Parses one adapter response line."
  [line]
  (let [[tag & values] (str/split line #"\s+")]
    (case tag
      "ok" {:tag :ok}
      "value" {:tag :value
               :value (let [value (first values)]
                        (when-not (= "null" value)
                          (parse-long-token value)))}
      "applied" {:tag :applied :applied (= "true" (first values))}
      "set" {:tag :set :values (mapv parse-long-token values)}
      "pairs" {:tag :pairs :values (mapv parse-pair-token values)}
      "triples" {:tag :triples :values (mapv parse-triple-token values)}
      "counts" {:tag :counts
                :exact (parse-long-token (first values))
                :scan (parse-long-token (second values))}
      "batch" {:tag :batch
               :inserted-count (parse-long-token (first values))
               :error-index (parse-long-token (second values))}
      "error" (let [message (if (< 6 (count line))
                              (subs line 6)
                              "adapter returned error")]
                (throw (ex-info message {:line line})))
      (throw (ex-info "Unknown adapter response" {:line line})))))

(defn adapter-command!
  "Sends one command to the local mqlite Jepsen adapter."
  [host port command]
  (with-open [socket (connect-socket! host (current-port port))
              reader (io/reader socket)
              writer (io/writer socket)]
    (.write writer (str command "\n"))
    (.flush writer)
    (if-let [line (.readLine reader)]
      (parse-response line)
      (throw (ex-info "adapter closed connection" {:command command})))))

(defn ensure-workload-schema!
  "Creates workload-specific indexes before concurrent client traffic starts."
  [{:keys [host port workload]}]
  (case workload
    "unique-index" (adapter-command! host port "ensure-unique-index")
    "secondary-index" (adapter-command! host port "ensure-secondary-index")
    "delete-set" (adapter-command! host port
                                    (str "seed-delete-set " delete-doc-count))
    "index-build" (adapter-command! host port
                                     (str "seed-index-build "
                                          index-build-seed-count))
    "drop-index" (adapter-command! host port
                                   (str "seed-drop-index "
                                        drop-index-doc-count))
    "compound-index" (adapter-command! host port "ensure-compound-index")
    "multikey-index" (adapter-command! host port "ensure-multikey-index")
    "find-and-modify-claim" (adapter-command! host port
                                              (str "seed-claim-jobs "
                                                   claim-job-count))
    "long-scan-snapshot" (adapter-command! host port
                                           (str "seed-long-scan "
                                                long-scan-doc-count))
    "write-batch-prefix" (adapter-command! host port
                                           "ensure-batch-prefix-index")
    nil)
  nil)

(defn value-token
  "Encodes a nullable integer for the adapter protocol."
  [value]
  (if (nil? value) "null" (str value)))

(defrecord MqliteClient [host port coll-name]
  client/Client
  (open! [this _test _node] this)

  (setup! [this _test] this)

  (invoke! [_this _test op]
    (try
      (case (:f op)
        :read (if (= "set" coll-name)
                (let [response (adapter-command! host port "read-set")]
                  (assoc op :type :ok :value (:values response)))
                (let [[k _] (:value op)
                      response (adapter-command! host port
                                                 (str "read-register " k))]
                  (assoc op
                         :type :ok
                         :value (independent/tuple k (:value response)))))
        :write (let [[k v] (:value op)]
                 (adapter-command! host port
                                   (str "write-register " k " " v))
                 (assoc op :type :ok))
        :cas (let [[k [old new]] (:value op)
                   response (adapter-command! host port
                                              (str "cas-register "
                                                   k " "
                                                   (value-token old)
                                                   " " new))]
               (assoc op :type (if (:applied response) :ok :fail)))
        :add (do
               (adapter-command! host port (str "add-set " (:value op)))
               (assoc op :type :ok))
        :unique-insert (let [[id value] (:value op)
                             response (adapter-command! host port
                                                        (str "unique-insert "
                                                             id " " value))]
                         (assoc op :type
                                (if (:applied response) :ok :fail)))
        :unique-read (let [response (adapter-command! host port "read-unique")]
                       (assoc op :type :ok :value (:values response)))
        :secondary-upsert (let [[id value] (:value op)]
                            (adapter-command! host port
                                              (str "secondary-upsert "
                                                   id " " value))
                            (assoc op :type :ok))
        :secondary-delete (do
                            (adapter-command! host port
                                              (str "secondary-delete "
                                                   (:value op)))
                            (assoc op :type :ok))
        :secondary-check (let [x (:value op)
                               indexed (adapter-command! host port
                                                         (str "secondary-index-read "
                                                              x))
                               scanned (adapter-command! host port
                                                        "secondary-scan")
                               scan-values (->> (:values scanned)
                                                (filter #(= x (second %)))
                                                (map first)
                                                sort
                                                vec)
                               index-values (vec (sort (:values indexed)))]
                           (assoc op :type :ok
                                  :value {:x x
                                          :index index-values
                                          :scan scan-values}))
        :read-your-writes (let [[id value] (:value op)
                                response (adapter-command! host port
                                                           (str "read-your-writes "
                                                                id " " value))]
                            (if (= value (:value response))
                              (assoc op :type :ok)
                              (assoc op :type :fail
                                     :observed (:value response))))
        :delete-set (do
                      (adapter-command! host port
                                        (str "delete-set " (:value op)))
                      (assoc op :type :ok))
        :delete-read (let [response (adapter-command! host port
                                                      "read-delete-set")]
                       (assoc op :type :ok :value (:values response)))
        :namespace-add (let [[namespace value] (:value op)]
                         (adapter-command! host port
                                           (str "namespace-add "
                                                (name namespace)
                                                " "
                                                value))
                         (assoc op :type :ok))
        :namespace-read (let [namespace (:value op)
                              response (adapter-command! host port
                                                         (str "namespace-read "
                                                              (name namespace)))]
                          (assoc op :type :ok
                                 :value {:namespace namespace
                                         :values (:values response)}))
        :count-upsert (let [[id value] (:value op)]
                        (adapter-command! host port
                                          (str "count-upsert " id " " value))
                        (assoc op :type :ok))
        :count-delete (do
                        (adapter-command! host port
                                          (str "count-delete " (:value op)))
                        (assoc op :type :ok))
        :count-check (let [response (adapter-command! host port
                                                      "count-check")]
                       (assoc op :type :ok
                              :value {:exact (:exact response)
                                      :scan (:scan response)}))
        :index-build-create (do
                              (adapter-command! host port
                                                "index-build-create")
                              (assoc op :type :ok))
        :index-build-upsert (let [[id value] (:value op)]
                              (adapter-command! host port
                                                (str "index-build-upsert "
                                                     id " " value))
                              (assoc op :type :ok))
        :index-build-delete (do
                              (adapter-command! host port
                                                (str "index-build-delete "
                                                     (:value op)))
                              (assoc op :type :ok))
        :index-build-check (let [x (:value op)
                                 indexed (adapter-command! host port
                                                           (str "index-build-index-read "
                                                                x))
                                 scanned (adapter-command! host port
                                                          "index-build-scan")
                                 scan-values (->> (:values scanned)
                                                  (filter #(= x (second %)))
                                                  (map first)
                                                  sort
                                                  vec)
                                 index-values (vec (sort (:values indexed)))]
                             (assoc op :type :ok
                                    :value {:x x
                                            :index index-values
                                            :scan scan-values}))
        :drop-index-create (do
                             (adapter-command! host port
                                               "drop-index-create")
                             (assoc op :type :ok))
        :drop-index-drop (do
                           (adapter-command! host port
                                             "drop-index-drop")
                           (assoc op :type :ok))
        :drop-index-upsert (let [[id value] (:value op)]
                             (adapter-command! host port
                                               (str "drop-index-upsert "
                                                    id " " value))
                             (assoc op :type :ok))
        :drop-index-delete (do
                             (adapter-command! host port
                                               (str "drop-index-delete "
                                                    (:value op)))
                             (assoc op :type :ok))
        :drop-index-check (let [x (:value op)
                                indexed (adapter-command! host port
                                                          (str "drop-index-index-read "
                                                               x))
                                scanned (adapter-command! host port
                                                         "drop-index-scan")
                                scan-values (->> (:values scanned)
                                                 (filter #(= x (second %)))
                                                 (map first)
                                                 sort
                                                 vec)
                                index-values (vec (sort (:values indexed)))]
                            (assoc op :type :ok
                                   :value {:x x
                                           :index index-values
                                           :scan scan-values}))
        :compound-upsert (let [[id a b] (:value op)]
                           (adapter-command! host port
                                             (str "compound-upsert "
                                                  id " " a " " b))
                           (assoc op :type :ok))
        :compound-delete (do
                           (adapter-command! host port
                                             (str "compound-delete "
                                                  (:value op)))
                           (assoc op :type :ok))
        :compound-check (let [[a b] (:value op)
                              indexed (adapter-command! host port
                                                        (str "compound-index-read "
                                                             a " " b))
                              scanned (adapter-command! host port
                                                       "compound-scan")
                              scan-values (->> (:values scanned)
                                               (filter #(and (= a (second %))
                                                             (= b (nth % 2))))
                                               (map first)
                                               sort
                                               vec)
                              index-values (vec (sort (:values indexed)))]
                          (assoc op :type :ok
                                 :value {:a a
                                         :b b
                                         :index index-values
                                         :scan scan-values}))
        :multikey-upsert (let [[id value] (:value op)]
                           (adapter-command! host port
                                             (str "multikey-upsert "
                                                  id " " value))
                           (assoc op :type :ok))
        :multikey-delete (do
                           (adapter-command! host port
                                             (str "multikey-delete "
                                                  (:value op)))
                           (assoc op :type :ok))
        :multikey-check (let [tag (:value op)
                              indexed (adapter-command! host port
                                                        (str "multikey-index-read "
                                                             tag))
                              scanned (adapter-command! host port
                                                       "multikey-scan")
                              scan-values (->> (:values scanned)
                                               (filter #(= tag (second %)))
                                               (map first)
                                               sort
                                               vec)
                              index-values (vec (sort (:values indexed)))]
                          (assoc op :type :ok
                                 :value {:tag tag
                                         :index index-values
                                         :scan scan-values}))
        :claim-job (let [worker (:value op)
                         response (adapter-command! host port
                                                    (str "claim-job " worker))]
                     (assoc op :type :ok
                            :value {:worker worker
                                    :job (:value response)}))
        :claim-read (let [response (adapter-command! host port "read-claims")]
                      (assoc op :type :ok :value (:values response)))
        :long-scan-advance (do
                             (adapter-command! host port
                                               (str "long-scan-advance "
                                                    (:value op)))
                             (assoc op :type :ok))
        :long-scan-read (let [response (adapter-command! host port
                                                         "long-scan-read")]
                          (if (<= (count (:values response)) 1)
                            (assoc op :type :ok
                                   :value (:values response))
                            (assoc op :type :fail
                                   :value (:values response))))
        :write-batch-prefix (let [base (:value op)
                                  response (adapter-command! host port
                                                             (str "write-batch-prefix "
                                                                  base))]
                              (if (and (= batch-prefix-ok-count
                                          (:inserted-count response))
                                       (= batch-prefix-error-index
                                          (:error-index response)))
                                (assoc op :type :ok)
                                (assoc op :type :fail
                                       :value {:base base
                                               :inserted-count
                                               (:inserted-count response)
                                               :error-index
                                               (:error-index response)})))
        :batch-prefix-read (let [response (adapter-command! host port
                                                            "read-batch-prefix")]
                             (assoc op :type :ok :value (:values response)))
        (throw (ex-info "Unsupported operation" {:op op})))
      (catch Exception e
        (op-info op e))))

  (teardown! [this _test] this)

  (close! [_this _test])

  client/Reusable
  (reusable? [_this _test] true))

(defn mqlite-client
  "Builds a Jepsen client for one workload collection."
  [opts db coll-name]
  (map->MqliteClient
    {:host (:host opts)
     :port (:port db)
     :coll-name coll-name}))

(defn register-workload
  "Builds the Jepsen linearizable-register workload."
  [opts]
  (let [partial (register/test {:nodes ["n1"] :per-key-limit 30})
        gen (recovering-generator opts (:generator partial))]
    {:checker (checker/compose {:register (client-only-checker
                                             (:checker partial))})
     :generator gen}))

(defn set-workload
  "Builds the Jepsen acknowledged-insert set workload."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [x] {:f :add :value x}) (range))
                               (repeat {:f :read})]))
        final-gen (gen/clients
                    (gen/each-thread
                      (gen/until-ok {:f :read})))]
    {:checker (checker/compose {:set (checker/set)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defrecord UniqueIndexChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          acknowledged (->> client-history
                            (filter #(and (= :unique-insert (:f %))
                                          (= :ok (:type %))))
                            (map :value)
                            set)
          final-read (->> client-history
                          (filter #(and (= :unique-read (:f %))
                                        (= :ok (:type %))))
                          last)
          final-pairs (set (:value final-read))
          lost (sort (set/difference acknowledged final-pairs))
          duplicates (->> final-pairs
                          (group-by second)
                          (filter #(< 1 (count (second %))))
                          (map (fn [[value pairs]]
                                 {:value value :pairs (sort pairs)}))
                          vec)]
      {:valid? (and (some? final-read)
                    (empty? lost)
                    (empty? duplicates))
       :acknowledged-count (count acknowledged)
       :final-count (count final-pairs)
       :lost lost
       :duplicate-values duplicates})))

(defrecord SecondaryIndexChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [checks (->> (h/client-ops history)
                      (filter #(and (= :secondary-check (:f %))
                                    (= :ok (:type %)))))
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [value (:value op)
                                        indexed (set (:index value))
                                        scanned (set (:scan value))]
                                    (when (not= indexed scanned)
                                      {:x (:x value)
                                       :index (sort indexed)
                                       :scan (sort scanned)}))))
                          vec)]
      {:valid? (and (pos? (count checks))
                    (empty? mismatches))
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord ReadYourWritesChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [ops (->> (h/client-ops history)
                   (filter #(= :read-your-writes (:f %))))
          failures (->> ops
                        (filter #(= :fail (:type %)))
                        vec)
          ok-count (count (filter #(= :ok (:type %)) ops))]
      {:valid? (and (pos? ok-count)
                    (empty? failures))
       :ok-count ok-count
       :failure-count (count failures)
       :failures (take 20 failures)})))

(defrecord DeleteSetChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          acknowledged (->> client-history
                            (filter #(and (= :delete-set (:f %))
                                          (= :ok (:type %))))
                            (map :value)
                            set)
          final-read (->> client-history
                          (filter #(and (= :delete-read (:f %))
                                        (= :ok (:type %))))
                          last)
          final-values (set (:value final-read))
          resurrected (sort (set/intersection acknowledged final-values))]
      {:valid? (and (some? final-read)
                    (empty? resurrected))
       :acknowledged-deletes (count acknowledged)
       :final-count (count final-values)
       :resurrected resurrected})))

(defrecord NamespaceIsolationChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          acknowledged (->> client-history
                            (filter #(and (= :namespace-add (:f %))
                                          (= :ok (:type %))))
                            (map :value))
          ack-a (set (map second (filter #(= :a (first %)) acknowledged)))
          ack-b (set (map second (filter #(= :b (first %)) acknowledged)))
          final-reads (->> client-history
                           (filter #(and (= :namespace-read (:f %))
                                         (= :ok (:type %)))))
          final-a-read (->> final-reads
                            (filter #(= :a (get-in % [:value :namespace])))
                            last)
          final-b-read (->> final-reads
                            (filter #(= :b (get-in % [:value :namespace])))
                            last)
          final-a (set (get-in final-a-read [:value :values]))
          final-b (set (get-in final-b-read [:value :values]))
          lost-a (sort (set/difference ack-a final-a))
          lost-b (sort (set/difference ack-b final-b))
          crossed-into-a (sort (set/intersection ack-b final-a))
          crossed-into-b (sort (set/intersection ack-a final-b))]
      {:valid? (and (some? final-a-read)
                    (some? final-b-read)
                    (pos? (count ack-a))
                    (pos? (count ack-b))
                    (empty? lost-a)
                    (empty? lost-b)
                    (empty? crossed-into-a)
                    (empty? crossed-into-b))
       :acknowledged-a (count ack-a)
       :acknowledged-b (count ack-b)
       :final-a-count (count final-a)
       :final-b-count (count final-b)
       :lost-a lost-a
       :lost-b lost-b
       :crossed-into-a crossed-into-a
       :crossed-into-b crossed-into-b})))

(defrecord CountConsistencyChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [checks (->> (h/client-ops history)
                      (filter #(and (= :count-check (:f %))
                                    (= :ok (:type %)))))
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [{:keys [exact scan]} (:value op)]
                                    (when (not= exact scan)
                                      {:exact exact :scan scan}))))
                          vec)]
      {:valid? (and (pos? (count checks))
                    (empty? mismatches))
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord IndexBuildChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          checks (->> client-history
                      (filter #(and (= :index-build-check (:f %))
                                    (= :ok (:type %)))))
          create-ok-count (->> client-history
                               (filter #(and (= :index-build-create (:f %))
                                             (= :ok (:type %))))
                               count)
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [value (:value op)
                                        indexed (set (:index value))
                                        scanned (set (:scan value))]
                                    (when (not= indexed scanned)
                                      {:x (:x value)
                                       :index (sort indexed)
                                       :scan (sort scanned)}))))
                          vec)]
      {:valid? (and (pos? create-ok-count)
                    (pos? (count checks))
                    (empty? mismatches))
       :create-ok-count create-ok-count
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord DropIndexChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          checks (->> client-history
                      (filter #(and (= :drop-index-check (:f %))
                                    (= :ok (:type %)))))
          create-ok-count (->> client-history
                               (filter #(and (= :drop-index-create (:f %))
                                             (= :ok (:type %))))
                               count)
          drop-ok-count (->> client-history
                             (filter #(and (= :drop-index-drop (:f %))
                                           (= :ok (:type %))))
                             count)
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [value (:value op)
                                        indexed (set (:index value))
                                        scanned (set (:scan value))]
                                    (when (not= indexed scanned)
                                      {:x (:x value)
                                       :index (sort indexed)
                                       :scan (sort scanned)}))))
                          vec)]
      {:valid? (and (pos? create-ok-count)
                    (pos? drop-ok-count)
                    (pos? (count checks))
                    (empty? mismatches))
       :create-ok-count create-ok-count
       :drop-ok-count drop-ok-count
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord CompoundIndexChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [checks (->> (h/client-ops history)
                      (filter #(and (= :compound-check (:f %))
                                    (= :ok (:type %)))))
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [value (:value op)
                                        indexed (set (:index value))
                                        scanned (set (:scan value))]
                                    (when (not= indexed scanned)
                                      {:a (:a value)
                                       :b (:b value)
                                       :index (sort indexed)
                                       :scan (sort scanned)}))))
                          vec)]
      {:valid? (and (pos? (count checks))
                    (empty? mismatches))
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord MultikeyIndexChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [checks (->> (h/client-ops history)
                      (filter #(and (= :multikey-check (:f %))
                                    (= :ok (:type %)))))
          mismatches (->> checks
                          (keep (fn [op]
                                  (let [value (:value op)
                                        indexed (set (:index value))
                                        scanned (set (:scan value))]
                                    (when (not= indexed scanned)
                                      {:tag (:tag value)
                                       :index (sort indexed)
                                       :scan (sort scanned)}))))
                          vec)]
      {:valid? (and (pos? (count checks))
                    (empty? mismatches))
       :check-count (count checks)
       :mismatch-count (count mismatches)
       :mismatches (take 20 mismatches)})))

(defrecord ClaimChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          claims (->> client-history
                      (filter #(and (= :claim-job (:f %))
                                    (= :ok (:type %))
                                    (some? (get-in % [:value :job]))))
                      (map :value)
                      vec)
          jobs (map :job claims)
          duplicate-jobs (->> claims
                              (group-by :job)
                              (filter #(< 1 (count (second %))))
                              (map (fn [[job claims]]
                                     {:job job
                                      :workers (sort (map :worker claims))}))
                              vec)
          final-read (->> client-history
                          (filter #(and (= :claim-read (:f %))
                                        (= :ok (:type %))))
                          last)
          final-claims (set (:value final-read))
          acknowledged (set (map (fn [{:keys [job worker]}] [job worker])
                                 claims))
          lost (sort (set/difference acknowledged final-claims))]
      {:valid? (and (some? final-read)
                    (pos? (count claims))
                    (empty? duplicate-jobs)
                    (empty? lost))
       :acknowledged-claims (count claims)
       :unique-claimed-jobs (count (set jobs))
       :duplicate-jobs duplicate-jobs
       :lost lost
       :final-count (count final-claims)})))

(defrecord LongScanSnapshotChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [reads (->> (h/client-ops history)
                     (filter #(= :long-scan-read (:f %))))
          failures (->> reads
                        (filter #(= :fail (:type %)))
                        vec)
          ok-count (count (filter #(= :ok (:type %)) reads))]
      {:valid? (and (pos? ok-count)
                    (empty? failures))
       :ok-count ok-count
       :failure-count (count failures)
       :failures (take 20 failures)})))

(defrecord BatchPrefixChecker []
  checker/Checker
  (check [_this _test history _opts]
    (let [client-history (h/client-ops history)
          successful-bases (->> client-history
                                (filter #(and (= :write-batch-prefix (:f %))
                                              (= :ok (:type %))))
                                (map :value)
                                set)
          final-read (->> client-history
                          (filter #(and (= :batch-prefix-read (:f %))
                                        (= :ok (:type %))))
                          last)
          final-ids (set (:value final-read))
          expected-prefix (fn [base]
                            (let [id-base (* base 10)]
                              (set (range id-base
                                          (+ id-base batch-prefix-ok-count)))))
          expected-suffix (fn [base]
                            (let [id-base (* base 10)]
                              (set (range (+ id-base batch-prefix-error-index)
                                          (+ id-base batch-prefix-width)))))
          lost (->> successful-bases
                    (mapcat (fn [base]
                              (set/difference (expected-prefix base)
                                              final-ids)))
                    sort)
          leaked-suffix (->> successful-bases
                             (mapcat (fn [base]
                                       (set/intersection (expected-suffix base)
                                                         final-ids)))
                             sort)]
      {:valid? (and (some? final-read)
                    (pos? (count successful-bases))
                    (empty? lost)
                    (empty? leaked-suffix))
       :successful-batches (count successful-bases)
       :lost-prefix-ids lost
       :leaked-suffix-ids leaked-suffix
       :final-count (count final-ids)})))

(defn unique-index-workload
  "Builds a workload checking unique index admission and durability."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :unique-insert
                                       :value [id (mod id unique-key-count)]})
                                    (range))
                               (repeat {:f :unique-read})]))
        final-gen (gen/clients
                    (gen/each-thread
                      (gen/until-ok {:f :unique-read})))]
    {:checker (checker/compose {:unique-index (->UniqueIndexChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn secondary-index-workload
  "Builds a workload checking final indexed reads against full scans."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :secondary-upsert
                                       :value [(mod id secondary-doc-count)
                                               (mod id secondary-key-count)]})
                                    (range))
                               (map (fn [id]
                                      {:f :secondary-delete
                                       :value (mod id secondary-doc-count)})
                                    (range))]))
        final-gen (gen/clients
                    (for [_round (range secondary-final-rounds)
                          x (range secondary-key-count)]
                      {:f :secondary-check :value x}))]
    {:checker (checker/compose {:secondary-index
                                (->SecondaryIndexChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn read-your-writes-workload
  "Builds a workload checking immediate visibility of acknowledged writes."
  [opts]
  (let [client-gen (gen/clients
                     (map (fn [id]
                            {:f :read-your-writes
                             :value [(mod id count-doc-count) id]})
                          (range)))]
    {:checker (checker/compose {:read-your-writes
                                (->ReadYourWritesChecker)})
     :generator (recovering-generator opts client-gen)}))

(defn delete-set-workload
  "Builds a workload checking acknowledged deletes do not resurrect."
  [opts]
  (let [client-gen (gen/clients
                     (map (fn [id]
                            {:f :delete-set
                             :value (mod id delete-doc-count)})
                          (range)))
        final-gen (gen/clients
                    (gen/each-thread
                      (gen/until-ok {:f :delete-read})))]
    {:checker (checker/compose {:delete-set (->DeleteSetChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn namespace-isolation-workload
  "Builds a workload checking concurrent collections stay isolated."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :namespace-add
                                       :value [:a id]})
                                    (range))
                               (map (fn [id]
                                      {:f :namespace-add
                                       :value [:b (+ namespace-b-offset id)]})
                                    (range))]))
        final-gen (gen/clients
                    [{:f :namespace-read :value :a}
                     {:f :namespace-read :value :b}])]
    {:checker (checker/compose {:namespace-isolation
                                (->NamespaceIsolationChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn count-consistency-workload
  "Builds a workload checking final counts against full scans."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :count-upsert
                                       :value [(mod id count-doc-count) id]})
                                    (range))
                               (map (fn [id]
                                      {:f :count-delete
                                       :value (mod id count-doc-count)})
                                    (range))]))
        final-gen (gen/clients
                    (map (fn [_] {:f :count-check})
                         (range count-final-rounds)))]
    {:checker (checker/compose {:count-consistency
                                (->CountConsistencyChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn index-build-workload
  "Builds a workload checking online index builds and final index consistency."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(repeat {:f :index-build-create})
                               (map (fn [id]
                                      {:f :index-build-upsert
                                       :value [(mod id index-build-doc-count)
                                               (mod id secondary-key-count)]})
                                    (range))
                               (map (fn [id]
                                      {:f :index-build-delete
                                       :value (mod id index-build-doc-count)})
                                    (range))]))
        final-gen (gen/phases
                    (gen/clients
                      (gen/each-thread
                        (gen/until-ok {:f :index-build-create})))
                    (gen/clients
                      (for [_round (range index-build-final-rounds)
                            x (range secondary-key-count)]
                        {:f :index-build-check :value x})))]
    {:checker (checker/compose {:index-build (->IndexBuildChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn drop-index-workload
  "Builds a workload checking drop/create index races and final consistency."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(repeat {:f :drop-index-create})
                               (repeat {:f :drop-index-drop})
                               (map (fn [id]
                                      {:f :drop-index-upsert
                                       :value [(mod id drop-index-doc-count)
                                               (mod id secondary-key-count)]})
                                    (range))
                               (map (fn [id]
                                      {:f :drop-index-delete
                                       :value (mod id drop-index-doc-count)})
                                    (range))]))
        final-gen (gen/phases
                    (gen/clients
                      (gen/each-thread
                        (gen/until-ok {:f :drop-index-create})))
                    (gen/clients
                      (for [_round (range drop-index-final-rounds)
                            x (range secondary-key-count)]
                        {:f :drop-index-check :value x})))]
    {:checker (checker/compose {:drop-index (->DropIndexChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn compound-index-workload
  "Builds a workload checking compound-index reads against full scans."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :compound-upsert
                                       :value [(mod id compound-doc-count)
                                               (mod id compound-a-count)
                                               (mod (quot id compound-a-count)
                                                    compound-b-count)]})
                                    (range))
                               (map (fn [id]
                                      {:f :compound-delete
                                       :value (mod id compound-doc-count)})
                                    (range))]))
        final-gen (gen/clients
                    (for [_round (range compound-final-rounds)
                          a (range compound-a-count)
                          b (range compound-b-count)]
                      {:f :compound-check :value [a b]}))]
    {:checker (checker/compose {:compound-index
                                (->CompoundIndexChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn multikey-index-workload
  "Builds a workload checking multikey-index reads against full scans."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [id]
                                      {:f :multikey-upsert
                                       :value [(mod id multikey-doc-count)
                                               (mod id secondary-key-count)]})
                                    (range))
                               (map (fn [id]
                                      {:f :multikey-delete
                                       :value (mod id multikey-doc-count)})
                                    (range))]))
        final-gen (gen/clients
                    (for [_round (range multikey-final-rounds)
                          tag (range secondary-key-count)]
                      {:f :multikey-check :value tag}))]
    {:checker (checker/compose {:multikey-index
                                (->MultikeyIndexChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn find-and-modify-claim-workload
  "Builds a workload checking atomic find-and-modify job claiming."
  [opts]
  (let [client-gen (gen/clients
                     (map (fn [worker]
                            {:f :claim-job :value worker})
                          (range)))
        final-gen (gen/clients
                    (gen/each-thread
                      (gen/until-ok {:f :claim-read})))]
    {:checker (checker/compose {:find-and-modify-claim
                                (->ClaimChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn long-scan-snapshot-workload
  "Builds a workload checking scan snapshots never observe partial epochs."
  [opts]
  (let [client-gen (gen/clients
                     (gen/mix [(map (fn [epoch]
                                      {:f :long-scan-advance
                                       :value epoch})
                                    (range))
                               (repeat {:f :long-scan-read})]))
        final-gen (gen/clients
                    (map (fn [_] {:f :long-scan-read})
                         (range long-scan-final-rounds)))]
    {:checker (checker/compose {:long-scan-snapshot
                                (->LongScanSnapshotChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn write-batch-prefix-workload
  "Builds a workload checking ordered insert_many prefix semantics."
  [opts]
  (let [client-gen (gen/clients
                     (map (fn [base]
                            {:f :write-batch-prefix
                             :value base})
                          (range)))
        final-gen (gen/clients
                    (gen/each-thread
                      (gen/until-ok {:f :batch-prefix-read})))]
    {:checker (checker/compose {:write-batch-prefix
                                (->BatchPrefixChecker)})
     :generator (recovering-generator opts client-gen final-gen)}))

(defn workload
  "Returns workload-specific checker and generator."
  [opts name]
  (case name
    "register" (register-workload opts)
    "set" (set-workload opts)
    "unique-index" (unique-index-workload opts)
    "secondary-index" (secondary-index-workload opts)
    "read-your-writes" (read-your-writes-workload opts)
    "delete-set" (delete-set-workload opts)
    "namespace-isolation" (namespace-isolation-workload opts)
    "count-consistency" (count-consistency-workload opts)
    "index-build" (index-build-workload opts)
    "drop-index" (drop-index-workload opts)
    "compound-index" (compound-index-workload opts)
    "multikey-index" (multikey-index-workload opts)
    "find-and-modify-claim" (find-and-modify-claim-workload opts)
    "long-scan-snapshot" (long-scan-snapshot-workload opts)
    "write-batch-prefix" (write-batch-prefix-workload opts)
    (throw (ex-info "Unsupported workload" {:workload name}))))

(defn test-map
  "Builds a complete Jepsen test map for a workload."
  [opts name]
  (let [db (mqlite-db opts name)
        workload (workload opts name)]
    {:name (str "mqlite-" name)
     :nodes ["n1"]
     :ssh {:dummy? true}
     :remote control/ssh
     :dummy? true
     :os os/noop
     :db db
     :client (mqlite-client opts db name)
     :nemesis (workload-nemesis opts db)
     :checker (:checker workload)
     :generator (:generator workload)
     :concurrency (:concurrency opts)
     :mqlite {:db-path (:db-path db)
              :log-path (:log-path db)
              :port (:port db)
              :workload name}}))

(defn workload-names
  "Expands the requested workload selector."
  [opts]
  (case (:workload opts)
    "all" ["register" "set" "unique-index" "secondary-index"
           "read-your-writes" "delete-set" "namespace-isolation"
           "count-consistency" "index-build" "drop-index"
           "compound-index" "multikey-index" "find-and-modify-claim"
           "long-scan-snapshot" "write-batch-prefix"]
    "register" ["register"]
    "set" ["set"]
    "unique-index" ["unique-index"]
    "secondary-index" ["secondary-index"]
    "read-your-writes" ["read-your-writes"]
    "delete-set" ["delete-set"]
    "namespace-isolation" ["namespace-isolation"]
    "count-consistency" ["count-consistency"]
    "index-build" ["index-build"]
    "drop-index" ["drop-index"]
    "compound-index" ["compound-index"]
    "multikey-index" ["multikey-index"]
    "find-and-modify-claim" ["find-and-modify-claim"]
    "long-scan-snapshot" ["long-scan-snapshot"]
    "write-batch-prefix" ["write-batch-prefix"]
    (throw (ex-info "Unsupported workload" {:workload (:workload opts)}))))

(defn run-workload!
  "Runs one Jepsen workload and returns its completed test."
  [opts name]
  (println "Running workload:" name)
  (let [result (jepsen/run! (test-map opts name))]
    (println "Result for" name ":")
    (pprint (:results result))
    result))

(defn valid-result?
  "Returns true when a completed Jepsen test is valid."
  [result]
  (true? (get-in result [:results :valid?])))

(defn configure-store!
  "Configures Jepsen's store directory."
  [opts]
  (alter-var-root #'store/base-dir
                  (constantly (canonical-path (:store-dir opts)))))

(defn -main
  "CLI entry point for the mqlite Jepsen suite."
  [& args]
  (let [opts (parse-args args)]
    (if (:help? opts)
      (do
        (println (usage))
        (System/exit 0))
      (do
        (configure-store! opts)
        (let [results (mapv #(run-workload! opts %) (workload-names opts))]
          (if (every? valid-result? results)
            (System/exit 0)
            (System/exit 1)))))))
