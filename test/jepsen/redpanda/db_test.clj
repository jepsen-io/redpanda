(ns jepsen.redpanda.db-test
  (:require [clojure [test :refer :all]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.redpanda.db :refer :all]
            [slingshot.slingshot :refer [try+ throw+]]))

(deftest cluster-info-parser-test
  (let [example "BROKERS
=======
ID    HOST             PORT
0*    192.168.122.101  9092
1     192.168.122.102  9092
2     192.168.122.103  9092

TOPICS
======
NAME  PARTITIONS  REPLICAS
t4    2           3"]
    (is (= {:brokers [{:id 0 :star? true :host "192.168.122.101" :port 9092}
                      {:id 1             :host "192.168.122.102" :port 9092}
                      {:id 2             :host "192.168.122.103" :port 9092}]
            :topics  [{:name "t4" :partitions 2 :replicas 3}]}
           (parse-cluster-info example)))))
