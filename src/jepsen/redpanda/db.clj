(ns jepsen.redpanda.db
  "Common functions across both Kafka and Redpanda.")

(defprotocol DB
  (node-id [db test node]
           "What's the ID of this node?")

  (topic-partition-state [db node topic-partition]
                         "Fetches information about the topic-partition state from this node."))
