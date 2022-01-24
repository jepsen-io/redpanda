(ns jepsen.redpanda.client
  "Wrapper for the Java Kafka client."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt]
            [jepsen.util :as util :refer [await-fn
                                          map-vals
                                          pprint-str]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.time Duration)
           (java.util Properties)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin Admin
                                           AdminClientConfig
                                           NewTopic)
           (org.apache.kafka.clients.consumer ConsumerConfig
                                              ConsumerRebalanceListener
                                              ConsumerRecord
                                              ConsumerRecords
                                              KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerConfig
                                              ProducerRecord)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.common.errors InvalidTopicException
                                           TopicExistsException)))

(def port
  "What port do we connect to?"
  9092)

(def next-transactional-id
  "We automatically assign each producer a unique transactional ID"
  (atom -1))

(defn new-transactional-id
  "Returns a unique transactional ID (mutating the global counter)"
  []
  (str "jt" (swap! next-transactional-id inc)))

(defn ^Properties ->properties
  "Turns a map into a Properties object."
  [m]
  (doto (Properties.)
    (.putAll (map-vals str m))))

(def consumer-config-logged?
  "Used to ensure that we only log consumer configs once."
  (atom false))

(def producer-config-logged?
  "Used to ensure that we only log producer configs once."
  (atom false))

(def consumer-group
  "Right now all consumers are a single consumer group."
  "jepsen-group")

(defn consumer-config
  "Constructs a properties map for talking to a given Kafka node."
  [node opts]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/consumer/ConsumerConfig.html
  ; And https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
  (cond->
    {ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
     "org.apache.kafka.common.serialization.LongDeserializer"

     ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
     "org.apache.kafka.common.serialization.LongDeserializer"

     ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG
     (str node ":" port)

     ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG
     1000

     ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG
     500

     ConsumerConfig/METADATA_MAX_AGE_CONFIG
     60000

     ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG
     10000

     ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG
     10000

     ConsumerConfig/HEARTBEAT_INTERVAL_MS_CONFIG
     300

     ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG
     6000 ; Bounded by server

     ConsumerConfig/CONNECTIONS_MAX_IDLE_MS_CONFIG
     60000

     ; ConsumerConfig/DEFAULT_ISOLATION_LEVEL
     ; ???
     }
    (:subscribe (:sub-via opts))
    (assoc ConsumerConfig/GROUP_ID_CONFIG consumer-group)

    (not= nil (:isolation-level opts))
    (assoc ConsumerConfig/ISOLATION_LEVEL_CONFIG (:isolation-level opts))

    (not= nil (:auto-offset-reset opts))
    (assoc ConsumerConfig/AUTO_OFFSET_RESET_CONFIG (:auto-offset-reset opts))

    (not= nil (:enable-auto-commit opts))
    (assoc ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG (:enable-auto-commit opts))))

(defn producer-config
  "Constructs a config map for talking to a given node."
  [node opts]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/producer/ProducerConfig.html
  ; See https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
  (cond-> {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG
           (str node ":" port)

           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
           ;"org.apache.kafka.common.serialization.StringSerializer"
           "org.apache.kafka.common.serialization.LongSerializer"

           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
           ;"org.apache.kafka.common.serialization.StringSerializer"
           "org.apache.kafka.common.serialization.LongSerializer"

           ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG 10000
           ; We choose this lower than DELIVERY_TIMEOUT_MS so that we have a
           ; chance to retry
           ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG 3000
           ProducerConfig/MAX_BLOCK_MS_CONFIG 10000
           ; Client sometimes complains `The configuration
           ; 'transaction.timeout.ms' was supplied but isn't a known config`;
           ; not sure what's up with that
           ProducerConfig/TRANSACTION_TIMEOUT_CONFIG
           1000
           ; We want rapid reconnects so we can observe broken-ness
           ProducerConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG 1000
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG 500
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000

           }
    (not= nil (:acks opts))
    (assoc ProducerConfig/ACKS_CONFIG (:acks opts))

    (not= nil (:idempotence opts))
    (assoc ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG (:idempotence opts))

    (not= nil (:retries opts))
    (assoc ProducerConfig/RETRIES_CONFIG (:retries opts))

    (not= nil (:transactional-id opts))
    (assoc ProducerConfig/TRANSACTIONAL_ID_CONFIG (:transactional-id opts))))

(defn admin-config
  "Constructs a config map for an admin client connected to the given node."
  [node]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/admin/AdminClientConfig.html
  {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG       (str node ":" port)
   AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG                 3000
   AdminClientConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG               1000
   AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG                     3000
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG     500
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000
   ; Never retry
   AdminClientConfig/RETRIES_CONFIG                                0})

(defn ^Duration ms->duration
  "Constructs a Duration from millis."
  [ms]
  (Duration/ofMillis ms))

(defn close!
  "Closes any AutoCloseable."
  [^java.lang.AutoCloseable c]
  (.close c))

(defn close-consumer!
  "Closes a consumer *immediately*. The default close claims it only blocks
  30s, but I've seen it deadlock for an hour."
  [^KafkaConsumer c]
  (.close c (ms->duration 0)))

(defn close-producer!
  "Closes a producer *immediately*, without waiting for incomplete requests."
  [^KafkaProducer p]
  (.close p (ms->duration 0)))

(defn consumer
  "Opens a new consumer for the given node."
  [opts node]
  (let [config (consumer-config node opts)]
    (when (compare-and-set! consumer-config-logged? false true)
      (info "Consumer config:\n" (pprint-str config)))
    (KafkaConsumer. (->properties config))))

(defn producer*
  "Opens a new producer for a node. Doesn't initialize transactions."
  [opts node]
  (let [config (producer-config node opts)]
    (when (compare-and-set! producer-config-logged? false true)
      (info "Producer config:\n" (pprint-str config)))
    (KafkaProducer. (->properties config))))

(defn producer
  "Opens a new producer for a node. Automatically initializes transactions, if
  :transactional-id opts is set."
  [opts node]
  (if-not (:transactional-id opts)
    (producer* opts node)
    ; initTransactions loves to explode for nondetermistic (possibly
    ; server-specific?) reasons, and when it does the entire producer winds up
    ; locked in an irrecoverable state, so we have to do this akward
    ; open-init-close dance
    (await-fn (fn init-txns []
                (let [p (producer* opts node)]
                  (try (.initTransactions p)
                       p
                       (catch Throwable t
                         (close-producer! p)
                         (throw t)))))
              {:log-interval 5000
               :log-message "Waiting for initTransactions()"})))

(defn admin
  "Opens an admin client for a node."
  [test node]
  (Admin/create (->properties (admin-config node))))

(defn create-topic!
  "Creates a new topic using an admin client. Synchronous. If the topic already
  exists, returns :already-exists instead of throwing."
  [^Admin admin name partitions replication-factor]
  (try
    (let [topic (NewTopic. ^String name,
                           ^int partitions,
                           ^short replication-factor)
          res   (.createTopics admin [topic])]
      (.. res values (get name) get))
    (catch java.util.concurrent.ExecutionException e
      (condp instance? (util/ex-root-cause e)
        TopicExistsException :already-exists
        (throw e)))))

(defn ^TopicPartition topic-partition
  "A tuple of a topic and a partition number together."
  [topic partition]
  (TopicPartition. topic partition))

(defn ^ProducerRecord producer-record
  "Constructs a ProducerRecord from a topic, partition, key, and value."
  [topic partition key value]
  (ProducerRecord. topic (int partition) key value))

(defn ^OffsetAndMetadata offset+metadata
  "Constructs an OffsetAndMetadata."
  [^long offset]
  (OffsetAndMetadata. offset))

(defn subscribe!
  "Subscribes to the given set of topics."
  ([^KafkaConsumer consumer, topics]
   (.subscribe consumer topics))
  ([^KafkaConsumer consumer, topics, rebalance-listener]
   (.subscribe consumer topics rebalance-listener)))

(defn poll-up-to
  "Takes a consumer, and polls it (with duration 0) for records up to and
  including (dec offset), and (quite possibly) higher. Returns a lazy sequence
  of ConsumerRecords. Helpful when you want to read everything up to at least
  the given offset. You can pass an offset from consumer.endOffsets(...) to
  this function directly to read everything in the topic up to that point.

  Assumes the consumer is subscribed to precisely one topic-partition.

  If offset is 0, returns nil. If offset is 1 (and consumer is seeked to 0),
  returns a single element (the one at offset 0), and possibly more elements
  after. If offset is 2, returns at least messages with offsets 0 and 1, and so
  on.

  TODO: for reasons I don't really understand, the first call here (with
  duration 1 ms) ALWAYS seems to return an empty list even when there's a bunch
  of records pending. Subsequent requests (with duration 100 ms) return the
  full set. Not sure what to do about this."
  ([consumer offset]
   (poll-up-to consumer offset (ms->duration 10)))
  ([^KafkaConsumer consumer offset duration]
   ; If the offset is zero, the partition is empty and we can return
   ; immediately.
   (when (pos? offset)
     (let [records     (.poll consumer duration)
           records     (vec records)
           last-record ^ConsumerRecord (peek records)]
       ;(info :poll-through-records offset records)
       (cond ; Empty window; we should poll with a longer duration next time.
             (nil? last-record)
             (poll-up-to consumer offset (ms->duration 100))

             ; We read far enough; we're done
             (<= (dec offset) (.offset last-record))
             records

             ; Possibly more to come
             true
             (concat records
                     (lazy-seq (poll-up-to consumer offset duration))))))))

(defn ^KafkaConsumer reset-to-last-committed-positions!
  "Takes a Consumer, and seeks back to the last offsets that were committed.
  Returns consumer. Adapted from
  https://github.com/apache/kafka/blob/7d9b9847f184ec72c4c80c046edc408789dcc066/examples/src/main/java/kafka/examples/ExactlyOnceMessageProcessor.java#L177-L184."
  [^KafkaConsumer consumer]
  (let [assignment (.assignment consumer)
        committed  (.committed consumer assignment)]
    (doseq [^TopicPartition topic-partition assignment]
      (if-let [^OffsetAndMetadata offset+metadata
               (.get committed topic-partition)]
        (.seek consumer topic-partition (.offset offset+metadata))
        (.seekToBeginning [topic-partition]))))
   consumer)

(defn abort-txn!
  "Aborts a transaction."
  [^KafkaProducer producer]
  (.abortTransaction producer))

(defmacro unwrap-errors
  "Depending on whether you're doing a future get or a sync call, Kafka might
  throw its exceptions wrapped in a j.u.c.ExecutionException. This macro
  transparently unwraps those."
  [& body]
  `(try ~@body
        (catch ExecutionException e#
          ; For debugging cases where the root exception gives us
          ; zero stacktrace info
          ; (throw e#)
          (let [cause# (util/ex-root-cause e#)]
            (if (instance? KafkaException cause#)
              (throw cause#)
              (throw e#))))))

(defn panicky-rebalance-listener
  "A ConsumerRebalanceListener which throws at the drop of a hat. We use this
  to make sure transactions aren't quietly having their topics/indices
  reassigned during execution."
  []
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ topic-partitions]
      (throw+ {:type       :partitions-revoked
               :partitions topic-partitions}))

    (onPartitionsAssigned [_ topic-partitions]
      (throw+ {:type       :partitions-assigned
               :partitions topic-partitions}))

    (onPartitionsLost [_ topic-partitions]
      (throw+ {:type       :partitions-lost
               :partitions topic-partitions}))))

(defn logging-rebalance-listener
  "A rebalance listener which journals each event to an atom containing a
  vector."
  [log-atom]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ topic-partitions]
      (swap! log-atom conj {:type :revoked
                            :partitions topic-partitions}))

    (onPartitionsAssigned [_ topic-partitions]
      (swap! log-atom conj {:type       :assigned
                            :partitions topic-partitions}))

    (onPartitionsLost [_ topic-partitions]
      (swap! log-atom conj {:type       :lost
                            :partitions topic-partitions}))))
