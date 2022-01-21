(ns jepsen.redpanda.db.kafka
  "Database automation for the Kafka database: setup, teardown, etc."
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
            [jepsen.redpanda.db :as redpanda.db]
            [jepsen.redpanda.db.redpanda :as rdb]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.apache.kafka.common TopicPartition)))

(def dir "Where do we install Kafka?"
  "/opt/kafka")

(def zk
  "Program that launches zookeeper."
  (str dir "/bin/zookeeper-server-start.sh"))

(def zk-config
  "Properties file for Zookeeper"
  (str dir "/config/zookeeper.properties"))

(def kafka-config
  "Properties file for Kafka"
  (str dir "/config/server.properties"))

(def kafka
  "Program that launches kafka."
  (str dir "/bin/kafka-server-start.sh"))
                                6 (109

(def kafka-log-file
  (str dir "/kafka.log"))

(def zk-log-file
  (str dir "/zk.log"))

(def kafka-pid-file
  (str dir "/kafka.pid"))

(def zk-pid-file
  (str dir "/zk.pid"))

(def kafka-data
  (str dir "/data/kafka"))

(def zk-data
  (str dir "/data/zk"))

(defn install!
  "Installs Kafka."
  [test]
  ; We ignore version and deb here; this is just a quick comparison against a
  ; hardcoded version.
  (let [url (str "https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz")]
    (c/su (cu/install-archive! url dir)
          ; Make data dirs
          (c/exec :mkdir :-p zk-data)
          (c/exec :mkdir :-p kafka-data))))

(defn configure!
  "Writes config files"
  [test node]
  (c/su
    ; Write node IDs
    (let [id (rdb/node-id test node)]
      (c/exec :echo id :> (str zk-data "/myid"))
      ; Replace ZK data dir
      (c/exec :sed :-i :-e (str "s/dataDir=.*/dataDir="
                                (str/escape zk-data {\/ "\\/"})
                                "/") zk-config)
      ; Add cluster nodes, init limit, etc to ZK config
      (let [nodes (->> (:nodes test)
                       (map (fn [node]
                              (let [id (rdb/node-id test node)]
                                (str "server." id "=" node ":2888:3888")))))
            conf (->> (concat nodes
                              ["tickTime=1000"
                               "initLimit=10"
                               "syncLimit=5"])
                      (str/join "\n"))]
        (c/exec :echo conf :>> zk-config))

      (c/trace
      ; Replace Kafka node ID
      (c/exec :sed :-i :-e (str "s/broker\\.id=.*/broker.id=" id "/") kafka-config))
      ; Set internal replication factors
      (c/exec :sed :-i :-e "s/offsets\\.topic\\.replication\\.factor=.*/offsets.topic.replication.factor=3/" kafka-config)
      (c/exec :sed :-i :-e "s/transaction\\.state\\.log\\.replication\\.factor=.*/transaction.state.log.replication.factor=3/" kafka-config)
      (c/exec :sed :-i :-e "s/transaction\\.state\\.log\\.min\\.isr=.*/transaction.state.log.min.isr=3/" kafka-config)
      ; And rebalance delay, to speed up startup
      (c/exec :sed :-i :-e "s/group\\.initial\\.rebalance\\.delay\\.ms=.*/group.initial.rebalance.delay.ms=3000/" kafka-config)
      ; And data dir
      (c/exec :sed :-i :-e (str "s/log\\.dirs=.*/log.dirs="
                                (str/escape kafka-data {\/ "\\/"})
                                "/") kafka-config)
      ; We'll write our own in a second
      (c/exec :sed :-i :-e "s/zookeeper\\.connect=.*//" kafka-config)

      ; Add advertised listeners etc to Kafka settings
      (let [lines [(str "advertised.listeners=PLAINTEXT://" node ":9092")
                   (let [r (:default-topic-replications test)]
                     (when-not (nil? r)
                       (str "default.replication.factor=" r)))
                   ; Default ISR is too weak
                   "min.insync.replicas=2"
                   ; ZK nodes
                   (->> (:nodes test)
                        (map (fn [node] (str node ":2181")))
                        (str/join ",")
                        (str "zookeeper.connect="))]
            lines (str/join "\n" lines)]
        (c/exec :echo lines :>> kafka-config)))))

(defrecord DB [node-ids]
  db/DB
  (setup! [this test node]
    ; We need a full set of node IDs for this
    (rdb/gen-node-id! test node)
    (jepsen/synchronize test)

    (install! test)
    (configure! test node)
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su
      (c/exec :rm :-rf dir)))

  db/Process
  (start! [this test node]
    (c/su
      (cu/start-daemon! {:chdir dir
                         :logfile zk-log-file
                         :pidfile zk-pid-file}
                        zk
                        zk-config)
      (cu/start-daemon! {:chdir   dir
                         :logfile kafka-log-file
                         :pidfile kafka-pid-file}
                        kafka
                        kafka-config)))

  (kill! [this test node]
    (c/su
      (cu/stop-daemon! kafka kafka-pid-file)
      (cu/stop-daemon! zk    zk-pid-file)
      (cu/grepkill! "java")))

  db/Pause
  (pause! [this test node]
    (c/su
      (cu/grepkill! :stop :java)))

  (resume! [this test node]
    (c/su
      (cu/grepkill! :cont :java)))

  db/LogFiles
  (log-files [this test node]
    {zk-log-file    "zk.log"
     kafka-log-file "kafka.log"})

  redpanda.db/DB
  (node-id [this test node]
    (rdb/node-id test node))

  (topic-partition-state [this node topic-partition]
    :not-implemented))

(defn db
  "Constructs a Jepsen database object which knows how to set up and teardown a
  Kafka cluster."
  []
  (map->DB {:node-ids (atom {})}))
