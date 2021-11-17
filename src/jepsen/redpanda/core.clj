(ns jepsen.redpanda.core
  "Entry point for command line runner. Constructs tests and runs them."
  (:require [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.redpanda [db :as db]]
            [jepsen.redpanda.workload [list-append :as list-append]]))

(def workloads
  "A map of workload names to workload constructor functions."
  {:list-append list-append/workload})

(def logging-overrides
  "New logging levels for various Kafka packages--otherwise this test is going
  to be NOISY"
  {
   "org.apache.kafka.clients.admin.AdminClientConfig"                :warn
   "org.apache.kafka.clients.consumer.ConsumerConfig"                :warn
   "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator" :warn
   "org.apache.kafka.clients.consumer.internals.SubscriptionState"   :warn
   "org.apache.kafka.clients.consumer.KafkaConsumer"                 :warn
   "org.apache.kafka.clients.producer.KafkaProducer"                 :warn
   "org.apache.kafka.clients.producer.ProducerConfig"                :warn
   ; We're gonna get messages constantly about NOT_LEADER_OR_FOLLOWER whenever
   ; we create a topic
   "org.apache.kafka.clients.producer.internals.Sender"              :error
   "org.apache.kafka.common.metrics.Metrics"                         :warn
   "org.apache.kafka.common.utils.AppInfoParser"                     :warn
   })

(defn redpanda-test
  "Constructs a test for RedPanda from parsed CLI options."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)]
    (merge tests/noop-test
           opts
           {:name      (name workload-name)
            :db        (db/db)
            :os        debian/os
            :client    (:client workload)
            :generator (->> (:generator workload)
                            (gen/stagger    (/ (:rate opts)))
                            (gen/time-limit (:time-limit opts))
                            (gen/nemesis nil))
            :checker   (checker/compose
                         {:stats      (checker/stats)
                          :perf       (checker/perf)
                          :ex         (checker/unhandled-exceptions)
                          :workload   (:checker workload)})
            :logging {:overrides logging-overrides}})))

(def validate-non-neg
  [#(and (number? %) (not (neg? %))) "Must be non-negative"])

(def cli-opts
  "Command line options."
  [[nil "--rate HZ" "Target number of ops/sec"
    :default  40
    :parse-fn read-string
    :validate validate-non-neg]

   ["-w" "--workload NAME" "Which workload should we run?"
    :parse-fn keyword
    :default  :list-append
    :validate [workloads (cli/one-of workloads)]]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn redpanda-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
