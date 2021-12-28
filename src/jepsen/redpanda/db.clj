(ns jepsen.redpanda.db
  "Database automation: setup, teardown, some node-related nemesis operations."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [parse-long pprint-str meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.apache.kafka.common TopicPartition)))

(def user
  "What user do we run RPK commands as?"
  "redpanda")

(def data-dir
  "Where does Redpanda data live?"
  "/var/lib/redpanda/data")

(def pid-file
  "Where do we store the redpanda pid?"
  "/var/lib/redpanda/data/pid.lock")

(def log-file
  "Where do we send stdout/stderr logs?"
  "/var/log/redpanda.log")

(def config-file
  "Where does the redpanda config file live?"
  "/etc/redpanda/redpanda.yaml")

(def enabled-file
  "A file we use to tell whether redpanda is able to start or not."
  "/etc/redpanda/jepsen-enabled")

(def nofile
  "The ulimit number of files we apply to the redpanda process."
  (long (Math/pow 2 20))) ; ~ 1 million

(defn rpk!
  "Runs an RPK command, just like c/exec."
  [& args]
  (c/sudo user
          (c/exec :rpk args)))

(defn install!
  "Installs Redpanda on the local node."
  [test]
  (c/su
    (c/exec :curl :-1sLf "https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh" | :sudo :-E :bash)
    ; Wipe out config file so we don't re-use previous run settings
    (c/exec :rm :-f "/etc/redpanda/redpanda.yaml")
    (c/exec :apt-get :-y
            :--allow-downgrades
            :--allow-change-held-packages
            :-o "DPkg::options::=--force-confmiss"
            :--reinstall :install (str "redpanda=" (:version test)))
    (debian/install {:redpanda (:version test)})
    ; We're going to manage daemons ourselves
    (c/exec :systemctl :stop :redpanda)
    (c/exec :systemctl :stop :wasm_engine)
    (c/exec :systemctl :disable :redpanda)
    (c/exec :systemctl :disable :wasm_engine)
    ))

(defn gen-node-id!
  "Generates a new node ID for a node, mutating the node-ids atom in the test's
  DB. Returns that node ID. Node IDs are allocated so that all IDs for a given
  node have the same (mod id node-count) value."
  [test node]
  (-> test :db :node-ids
      (swap! (fn [node-ids]
               (let [id (or (when-let [id (get node-ids node)]
                              (+ id (count (:nodes test))))
                            (.indexOf ^java.util.List (:nodes test) node))]
                 (when (neg? id)
                   (throw+ {:type ::node-not-in-cluster
                            :node node
                            :nodes (:nodes test)}))
                 (assoc node-ids node id))))
      (get node)))

(defn node-id->node
  "Takes a test and a node ID and returns the node name that ID must have been
  for."
  [test node-id]
  (let [nodes (:nodes test)]
    (nth nodes (mod node-id (count nodes)))))

(defn node-id
  "Takes a test and a node name, and returns the current numeric ID (e.g. 0, 1,
  ...) for that node. The first node in the test is initially node 0, the root
  node."
  [test node]
  (-> test :db :node-ids deref (get node)))

(defn configure!
  "Sets up the local node's config. Pass initial? true when first bootstrapping
  the cluster."
  ([test node]
   (configure! test node false))
  ([test node initial?]
  (c/su
    (let [id (node-id test node)]
      ; If Redpanda creates a topic automatically, it might be under-replicated
      (let [e (:enable-server-auto-create-topics test)]
        (when-not (nil? e)
          (info "Setting redpanda.auto_create_topics_enabled" e)
          (rpk! :config :set "redpanda.auto_create_topics_enabled" false)))

      ; Redpanda's internal topic kafka_internal/group/0 only has replication
      ; factor of 1; you need to set it to 3 if you want actual fault
      ; tolerance for consumer groups.
      (let [r (:default-topic-replications test)]
        (when-not (nil? r)
          (info "Setting default-topic_replications" r)
          (rpk! :config :set "redpanda.default_topic_replications" r)))

      (rpk! :config :bootstrap
            :--id     id
            :--self   (cn/local-ip)
            ; On the initial run, the 0th node has an empty ips list and all
            ; other nodes join it. On subsequent runs, we join to every ip.
            (when-not (and initial? (zero? id))
              [:--ips (->> (:nodes test)
                           (remove #{node})
                           (map cn/ip)
                           (str/join ","))]))))))

(defn check-topic-creation
  "Checks that you can create a topic."
  []
  (c/su
    (let [topic "jepsen-test"
          res   (rpk! :topic :create topic)]
      (when-not (re-find #"\s+(OK|TOPIC_ALREADY_EXISTS)" res)
        (throw+ {:type ::topic-create-failed
                 :res  res}))
      ; Clean up if we can
      (meh (rpk! :topic :delete topic))
      topic)))

(defn await-topic-creation
  "Waits until you can create a topic."
  []
  (util/await-fn check-topic-creation
                 {:log-interval 10000
                  :log-message  "Waiting for topic creation"}))

(defn disable!
  "Disables starting redpanda on the current node."
  []
  (c/su (c/exec :rm :-f enabled-file)))

(defn enable!
  "Enables starting redpanda on the current node."
  []
  (c/su (c/exec :touch enabled-file)))

(defn enabled?
  "Can we start redpanda on this node?"
  []
  (cu/exists? enabled-file))

(defn nuke!
  "Kills the process, wipes data files, and makes it impossible to start this
  node. Leaves the log intact."
  [test node]
  (info "Nuking" node)
  (disable!)
  (db/kill! (:db test) test node)
  (c/su
    (c/exec :rm :-rf
            pid-file
            (c/lit (str data-dir "/*")))))

(defrecord DB [node-ids tcpdump]
  db/DB
  (setup! [this test node]
    ; Generate an initial node ID
    (info "Node ID" (gen-node-id! test node))
    (install! test)
    (enable!)
    (configure! test node true)

    (when (:tcpdump test)
      (db/setup! tcpdump test node))

    (c/su
      ; Make sure log file is ready
      (c/exec :touch log-file)
      (c/exec :chown (str user ":" user) log-file))

    ; Start primary
    (when (zero? (node-id test node))
      (db/start! this test node))
    (jepsen/synchronize test)

    ; Then secondaries
    (when-not (zero? (node-id test node))
      (db/start! this test node))

    ; Wait for cluster to be ready.
    (await-topic-creation))

  (teardown! [this test node]
    (nuke! test node)
    (c/su
      (c/exec :rm :-f log-file))
    (when (:tcpdump test)
      (db/teardown! tcpdump test node)))

  db/LogFiles
  (log-files [this test node]
    (concat (when (:tcpdump test)
              (db/log-files tcpdump test node))
            [log-file]))

  db/Process
  (start! [this test node]
    (if-not (enabled?)
      :disabled
      (do (c/sudo user
                  (cu/start-daemon!
                    {:chdir "/"
                     :logfile log-file
                     :pidfile pid-file
                     :make-pidfile? false}
                    "/usr/bin/redpanda"
                    :--redpanda-cfg config-file))
          ; Bump up filehandle limit
          (c/su (let [pid (util/await-fn
                            (fn get-pid []
                              (let [pid (c/exec :cat pid-file)]
                                (when-not (re-find #"\d+\n?" pid)
                                  (throw+ {:type :no-pid-in-file}))
                                pid))
                            {:log-message "waiting for startup to apply ulimit"
                             :log-interval 10000})]
                  (try+
                    (c/exec :prlimit (str "--nofile=" nofile) :--pid pid)
                    (catch [:type :jepsen.control/nonzero-exit] e
                      (if (re-find #"No such process" (:err e))
                        (throw+ {:type :ulimit-failed})
                        (throw+ e)))))))))


  (kill! [this test node]
    (c/su
      (cu/stop-daemon! :redpanda pid-file)))

  db/Pause
  (pause! [this test node]
    (c/su
      (cu/grepkill! :stop :redpanda)))

  (resume! [this test node]
    (c/su
      (cu/grepkill! :cont :redpanda))))

(defn db
  "Constructs a Jepsen database object which knows how to set up and tear down
  a Redpanda cluster."
  []
  (map->DB {:node-ids (atom {})
            :tcpdump (db/tcpdump {:ports [8082 9092 9644 33145]})}))

;; Cluster ops

(defn lowercase
  "Converts a string to lowercase."
  [^String s]
  (.toLowerCase s))

(defn part-str
  "Takes a string s and a seq of column widths. Returns a seq of substrings
  of s, each corresponding to one of those columns, plus one final column of
  unbounded width."
  ([s widths]
   (part-str s 0 widths []))
  ([s offset widths columns]
   (if-not (seq widths)
     ; Last column
     (conj columns (subs s offset))
     ; Intermediate column
     (let [[width & widths'] widths
           offset' (+ offset width)]
       (recur s offset' widths' (conj columns (subs s offset offset')))))))

(defn parse-header
  "Takes a string of whitespace-separated names and returns a seq of {:name
  :width} maps describing those columns. Names are lowercased keywords."
  ([s]
   (->> (re-seq #"[^\s]+\s*" s)
        (map (fn [^String column]
               {:name  (-> column str/trimr lowercase keyword)
                :width (.length column)})))))

(defn parse-tables
  "Parses a table structure like

      BROKERS
      =======
      ID    HOST             PORT
      0*    192.168.122.101  9092
      1     192.168.122.102  9092
      2     192.168.122.103  9092

      TOPICS
      ======
      NAME  PARTITIONS  REPLICAS
      t4    2           3

  Into a map like {:brokers [{:id \"0*\" :host \"192...\" ...} ...] ...}"
  [s]
  (loop [lines          (str/split s #"\n") ; Unconsumed lines
         section        nil      ; What section are we in?
         expect         :section ; What do we expect next?
         column-names   []       ; A seq of names for each column
         column-widths  []       ; A seq of how many characters wide each col is
         data           {}]      ; A map of sections to vectors of rows,
                                 ; each row a map of column names to strings
    (if-not (seq lines)
      data
      (let [[line & lines'] lines]
        (case expect
          :divider
          (do (assert+ (re-find #"^=+$" line)
                       {:type ::parse-error
                        :expected "A divider like ==="
                        :actual   line})
              (recur lines' section :header column-names column-widths data))

          :header
          (let [header (parse-header line)]
            (recur lines' section :row-or-blank
                   (mapv :name header)
                   (butlast (map :width header))
                   data))

          :row-or-blank
          (condp re-find line
            ; Blank
            #"^$"   (recur lines' nil :section [] [] data)
            ; Row
            (let [row (->> (part-str line column-widths)
                           (map str/trimr)
                           (zipmap column-names))
                  rows  (get data section [])
                  rows' (conj rows row)
                  data' (assoc data section rows')]
              (recur lines' section expect column-names column-widths data')))

          :section
          (recur lines' (-> line lowercase keyword) :divider [] [] data))))))

(defn parse-cluster-info
  "Parses a cluster info string."
  [s]
  (-> (parse-tables s)
      (update :brokers
              (partial map
                       (fn [broker]
                         (let [[_ id star] (re-find #"(\d+)(\*)?" (:id broker))]
                           (-> broker
                               (assoc :id (parse-long id))
                               (cond-> star (assoc :star? true))
                               (update :port parse-long))))))
      (update :topics
              (partial map
                       (fn [topic]
                         (-> topic
                             (update :partitions parse-long)
                             (update :replicas parse-long)))))))

(defn cluster-info
  "Returns cluster info from the current node as a clojure structure, by
  shelling out to rpk. Returns nil if node is unavailable."
  []
  (try+
    (parse-cluster-info (rpk! :cluster :info))
    (catch [:type :jepsen.control/nonzero-exit, :exit 1] e
      nil)))

(def common-http-opts
  "Common options for HTTP requests."
  {:socket-timeout     10000
   :connection-timeout 1000
   :as                 :json})

(defn broker-state
  "Fetches the broker state from the given node via HTTP."
  [node]
  (-> (http/get (str "http://" node ":9644/v1/brokers")
                common-http-opts)
      :body))

(defn topic-partition-state
  "Takes a node and a TopicPartition. Fetches the topic-partition data from
  this node's /partitions/kafka/<topic>/<partition> API. Topics must be
  URL-safe; we're not encoding them here."
  [node ^TopicPartition topic-partition]
  (:body (http/get (str "http://" node ":9644/v1/partitions/kafka/"
                        (.topic topic-partition) "/"
                        (.partition topic-partition))
                   common-http-opts)))

(defn decommission!
  "Asks a node `via` to decommission a node id `target`"
  [via target]
  (try+
    (-> (http/put (str "http://" via ":9644/v1/brokers/" target "/decommission")
                  common-http-opts)
        :body)
    (catch [:status 400] e
      ; Try parsing message as data structure
      (throw+ (update e :body json/parse-string true)))
    (catch [:status 500] e
      ; Try parsing message as data structure
      (throw+ (update e :body json/parse-string true)))))
