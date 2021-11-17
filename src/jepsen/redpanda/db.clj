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
    ;(c/exec :systemctl :disable :redpanda)
    ;(c/exec :systemctl :disable :wasm_engine)
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
        (c/exec :rm :-rf (c/lit "/var/lib/redpanda/data/*"))))

    db/LogFiles
    (log-files [this test node]
      [])

    db/Process
    (start! [this test node]
      (c/su
        (try+
          (c/exec :systemctl :start :redpanda)
          (catch [:type :jepsen.control/nonzero-exit, :exit 1] e
            (if (re-find #"control process exited with error code" (:err e))
              ; Let's check the journal to provide a more informative error msg
              (let [log (c/exec :journalctl :-u :redpanda | :tail :-n 12)]
                (throw+ {:type ::startup-crash
                         :log  log}
                        (str "Redpanda crashed on startup:\n\n"
                             log)))
              ; Nothing we can do here
              (throw+ e))))))

    (kill! [this test node]
      (c/su
        (try+
          (c/exec :systemctl :stop :redpanda)
          :killed
          (catch [:type :jepsen.control/nonzero-exit, :exit 5] e
            ; Unit not loaded
            :not-installed))))))
