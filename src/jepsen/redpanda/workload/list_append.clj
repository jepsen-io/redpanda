(ns jepsen.redpanda.workload.list-append
  "A workload which treats a Kafka topic as an ordered list of numbers, and
  performs transactional (or non-transactional) appends and reads of the entire
  list."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [client :as client]
                    [generator :as gen]
                    [util :as util :refer [pprint-str]]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.redpanda [client :as rc]])
  (:import (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.common.errors InvalidTopicException
                                           NotLeaderOrFollowerException
                                           TimeoutException
                                           UnknownTopicOrPartitionException
                                           )))

(def partition-count
  "How many partitions per topic?"
  2)

(defn k->topic
  "Turns a logical key into a topic."
  [k]
  (str "t" (quot k partition-count)))

(defn k->partition
  "Turns a logical key into a partition within a topic."
  [k]
  (mod k partition-count))

(defn k->topic-partition
  "Turns a logical key into a TopicPartition."
  [k]
  (rc/topic-partition (k->topic k) (k->partition k)))

(def replication-factor
  "What replication factor should we use for each topic?"
  3)

(defn mop!
  "Applies a micro-operation from a transaction: either a :r read or a :append
  operation."
  [{:keys [extant-topics admin producer consumer] :as client} [f k v :as mop]]
  (let [topic           (k->topic k)
        topic-partition (k->topic-partition k)]
    ; Create topic if it doesn't exist.
    (when-not (contains? @extant-topics topic)
      (rc/create-topic! admin topic partition-count replication-factor)
      (swap! extant-topics conj topic))

    (case f
      :r
      (try
        ; Start by assigning our consumer to this particular topic, seeking
        ; the consumer to the beginning, then reading the entire topic.
        (doto consumer
          (.assign [topic-partition])
          (.seekToBeginning [topic-partition]))

        ; How far do we have to read?
        (let [end-offset (-> consumer
                             (.endOffsets [topic-partition])
                             (get topic-partition))
              ; Read at least that far
              records (rc/poll-up-to consumer end-offset)
              ; Map records back into a list of integer elements
              elements (mapv (fn record->element [^ConsumerRecord r]
                               (.value r))
                             records)]
          [f k elements])
        (catch InvalidTopicException _
          ; This can happen when a topic is created on one side of a partition
          ; but another node doesn't know about it yet.
          [f k nil]))

      :append
      (let [record (rc/producer-record topic (k->partition k) nil v)
            res    @(.send producer record)]
        mop))))

(defrecord Client [; Our three Kafka clients
                   admin producer consumer
                   ; An atom with a set of topics we've created. We have to
                   ; create topics before they can be used.
                   extant-topics]
  client/Client
  (open! [this test node]
    (assoc this
           :admin     (rc/admin test node)
           :producer  (rc/producer test node)
           :consumer  (rc/consumer test node)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (let [txn  (:value op)
            txn' (mapv (partial mop! this) txn)]
        (assoc op :type :ok, :value txn'))
      (catch ExecutionException e
        (condp instance? (util/ex-root-cause e)
          InvalidTopicException
          (assoc op :type :fail, :error :invalid-topic)

          NotLeaderOrFollowerException
          (assoc op :type :fail, :error :not-leader-or-follower)

          ; Love that we have to catch this in two different ways
          TimeoutException
          (assoc op :type :info, :error :timeout)

          UnknownTopicOrPartitionException
          (assoc op :type :fail, :error :unknown-topic-or-partition)

          (throw e)))
      (catch TimeoutException e
        (assoc op :type :info, :error :timeout))))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (rc/close-producer! producer)
    (rc/close! consumer)))

(defn client
  "Constructs a fresh client for this workload."
  []
  (map->Client {:extant-topics (atom #{})}))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map. Options are:

    (none)

  ... plus those taken by jepsen.tests.cycle.append/test, e.g. :key-count,
  :min-txn-length, ..."
  [opts]
  (let [workload (append/test
                   (assoc opts
                          ; TODO: don't hardcode these
                          :max-txn-length 1
                          :consistency-models [:strict-serializable]))]
    (-> workload
        (assoc :client (client))
        ; Rewrite generator ops to use :f :read or :f :write if they're read or
        ; write-only. Elle doesn't care, but this helps us visualize read vs
        ; write perf better.
        (update :generator
                (fn wrap-gen [gen]
                  (gen/map (fn tag-rw [op]
                             (case (->> op :value (map first) set)
                               #{:r}      (assoc op :f :read)
                               #{:append} (assoc op :f :write)
                               op))
                           gen))))))
