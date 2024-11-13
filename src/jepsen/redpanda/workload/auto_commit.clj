(ns jepsen.redpanda.workload.auto-commit
  "There seems to be a good deal of disagreement about what auto-commit
  actually does. Some people think it allows commits of any offsets returned by
  .poll(). Some people think it only allows commits of the offsets polled
  *prior* to the most recent call to .poll().

  We have a single topic-partition. We append elements to it using .send(), and
  read elements with .poll(). We also ask for the current committed position
  using consumer.committed().

    {:type :invoke, :f :send}
    {:type :ok, :f :send}

    {:type :invoke, :f :poll}
    {:type :ok, :f :poll, :value [offset1 offset2 ...]}

    {:type :invoke, :f :committed}
    {:type :ok, :f committed, :value 4}"
  (:require [bifurcan-clj [core :as b]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ real-pmap loopr with-retry]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util :refer [map-vals
                                           meh
                                           nanos->secs
                                           pprint-str]]]
            [jepsen.checker.perf :as perf]
            [jepsen.tests kafka]
            [jepsen.redpanda [client :as rc]
                             [db :as db]]
            [jepsen.redpanda.workload.queue :as rq]
            [tesser.core :as t]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent ExecutionException)
           (java.time Duration)
           (org.apache.kafka.clients.admin Admin)
           (org.apache.kafka.clients.consumer ConsumerRecords
                                              ConsumerRecord
                                              KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer
                                              RecordMetadata)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.common.errors AuthorizationException
                                           DisconnectException
                                           InterruptException
                                           InvalidProducerEpochException
                                           InvalidReplicationFactorException
                                           InvalidTopicException
                                           InvalidTxnStateException
                                           NetworkException
                                           NotControllerException
                                           NotLeaderOrFollowerException
                                           OutOfOrderSequenceException
                                           ProducerFencedException
                                           TimeoutException
                                           UnknownTopicOrPartitionException
                                           UnknownServerException
                                           )))

(def topic
  "Our only topic."
  "t1")

(def topic-partition
  "Our only topic-partition"
  (rc/topic-partition topic 0))

(defn send!
  "Sends a message (always 1) to the topic, returning a promise of an offset."
  [^KafkaProducer producer]
  (let [fut (.send producer
                   (rc/producer-record topic 0 nil 1))]
    (delay
      (let [res ^RecordMetadata (-> fut
                                    (deref 1000 nil)
                                    (or (throw+ {:type :timeout})))]
        (when (.hasOffset res)
          (.offset res))))))

(defrecord Client [; What node are we bound to?
                   node
                   ; Our three Kafka clients
                   ^Admin admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer]
  client/Client
  (open! [this test node]
    (let [admin (rc/admin test node)
          consumer (rc/consumer (assoc test :max-poll-records 4) node)]

      (with-retry [tries 10]
        (rc/unwrap-errors
          (rc/create-topic! admin topic 1 rq/replication-factor))
        (catch InvalidReplicationFactorException e
          (when (= 0 tries)
            (throw e))
          (info "retrying topic creation")
          (Thread/sleep 1000)
          (retry (dec tries))))

      (rc/subscribe! consumer [topic])
      (assoc this
             :node          node
             :admin         admin
             :consumer     consumer
             :producer      (rc/producer test node))))

  (setup! [this test])

  (invoke! [this test {:keys [f value] :as op}]
    (rq/with-errors op
      (rc/unwrap-errors
        (case f
          :send
          (assoc op
                 :type :ok
                 :value (->> (range 5)
                             (mapv (fn [_] (send! producer)))
                             (mapv deref)))

          :poll
          (let [records (.poll consumer (rc/ms->duration rq/poll-ms))]
            (condp = (count (.partitions records))
              0 (assoc op :type :ok, :value nil)
              1 (let [tp (first (.partitions records))
                      _ (assert (= topic (.topic tp)))
                      offsets (mapv (fn [^ConsumerRecord record]
                                      (.offset record))
                                    (.records records tp))]
                  (assoc op :type :ok, :value offsets))
              (throw+ {:type :too-many-partitions
                       :partitons (.partitions records)})))

          :committed
          (let [tp->offset-meta (.committed consumer
                                            #{topic-partition}
                                            (Duration/ofSeconds 10))
                offset-meta (get tp->offset-meta topic-partition)]
            (assoc op
                   :type :ok,
                   :value (when offset-meta
                            (.offset offset-meta))))))))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (rc/close-producer! producer)
    (rc/close-consumer! consumer))

  client/Reusable
  (reusable? [this test]
             ; When we crash, we want to tear down our connections
             false))

(defn client
  "Constructs a fresh client for this workload."
  []
  (map->Client {}))

(defn generator
  "Repeatedly runs the topic generator, constrained to single threads."
  []
  (gen/reserve 1 (gen/repeat {:f :send})
               1 (gen/repeat {:f :poll})
               (gen/repeat {:f :committed})
               ))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (loopr [errs      [] ; Error maps
              sent      -1 ; The highest offset we sent
              polled    -1 ; The first offset we're currently processing
              committed -2 ; The committed offset
              ]
             [{:keys [type f value] :as op} history]
             (case f
               :send
               (if (not= type :ok)
                 (recur errs sent polled committed)
                 (recur errs
                        (max sent (peek value))
                        polled
                        committed))

               :poll
               (if (not= type :invoke)
                 (recur errs sent polled committed)
                 ; Look forward to figure out what we *will* poll
                 (let [op' (h/completion history op)]
                   (if (and (= (:type op') :ok)
                            (:value op'))
                     (recur errs
                            sent
                            (max polled (first (:value op')))
                            committed)
                     ; Something else
                     (recur errs sent polled committed))))

               ; If committed catches up to the values we most recently polled
               ; (and are presumably still processing), we've got a problem.
               :committed
               (if (or (nil? value)
                       (not= type :ok))
                 (recur errs sent polled committed)
                 ; Note that the committed offset returned from committed is
                 ; actually *not* committed, but the *following* offset.
                 (if (< polled (dec value))
                   (recur (conj errs
                                {:valid?     false
                                 :type       :committed-too-far
                                 :polled     polled
                                 :committed  value
                                 :op         op})
                          sent
                          polled
                          (max committed value))
                   (recur errs
                          sent
                          polled
                          (max committed value)))))
               ; Done
               {:valid? (not (seq errs))
                :errors errs}))))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map.

  These options must also be present in the test map, because they are used by
  the checker, client, etc at various points. For your convenience, they are
  included in the workload map returned from this function; merging that map
  into your test should do the trick."
  [opts]
  (merge opts
         {:generator (generator)
          :client    (client)
          :checker   (checker)}))
