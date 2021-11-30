(ns jepsen.redpanda.db
  "Database automation: setup, teardown, some node-related nemesis operations."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [pprint-str meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

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

(defn rpk!
  "Runs an RPK command, just like c/exec."
  [& args]
  (c/sudo user
          (c/exec :rpk args)))

(defn install!
  "Installs Redpanda on the local node."
  []
  (c/su
    (c/exec :curl :-1sLf "https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh" | :sudo :-E :bash)
    (debian/install [:redpanda])
    ; We're going to manage daemons ourselves
    (c/exec :systemctl :stop :redpanda)
    (c/exec :systemctl :stop :wasm_engine)
    (c/exec :systemctl :disable :redpanda)
    (c/exec :systemctl :disable :wasm_engine)
    ))

(defn node-id
  "Takes a test and a node name, and returns a numeric ID (e.g. 0, 1, ...) for
  that node. The first node in the test is node 0, the root node."
  [test node]
  (let [i (.indexOf ^java.util.List (:nodes test) node)]
    (when (neg? i)
      (throw+ {:type ::node-not-in-cluster
               :node node
               :nodes (:nodes test)}))
    i))

(defn configure!
  "Sets up the local node's config"
  [test node]
  (c/su
    (let [id (node-id test node)]
      (rpk! :config :bootstrap
            :--id     id
            :--self   (cn/local-ip)
            (when-not (zero? id)
              [:--ips (cn/ip (first (:nodes test)))])))))

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
                 {:log-message "Waiting for topic creation"}))

(defn db
  "Constructs a Jepsen database object which knows how to set up and tear down
  a Redpanda cluster."
  []
  (reify db/DB
    (setup! [this test node]
      (install!)
      (configure! test node)

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
      (db/kill! this test node)
      (c/su
        (c/exec :rm :-rf
                pid-file
                log-file
                (c/lit (str data-dir "/*")))))

    db/LogFiles
    (log-files [this test node]
      [log-file])

    db/Process
    (start! [this test node]
      (c/su
        ; Make sure log file is ready
        (c/exec :touch log-file)
        (c/exec :chown (str user ":" user) log-file)
        ; Ensure it's not running
        (try+ (let [pids (c/exec :pgrep :--list-full :redpanda)]
                (throw+ {:type :redpanda-running
                         :pids pids}))
              (catch [:type :jepsen.control/nonzero-exit, :exit 1] _)))
      ; Start!
      (c/sudo user
              (cu/start-daemon!
                {:chdir "/"
                 :logfile log-file
                 :pidfile pid-file
                 :make-pidfile? false}
                "/usr/bin/redpanda"
                :--redpanda-cfg config-file)))

    (kill! [this test node]
      (c/su
        (cu/stop-daemon! :redpanda pid-file)))

    db/Pause
    (pause! [this test node]
      (c/su
        (cu/grepkill! :stop :redpanda)))

    (resume! [this test node]
      (c/su
        (cu/grepkill! :cont :redpanda)))))
