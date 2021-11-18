(ns jepsen.redpanda.client
  "Wrapper for the Java Kafka client."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util :refer [map-vals
                                          pprint-str]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.time Duration)
           (java.util Properties)
           (org.apache.kafka.clients.admin Admin
                                           AdminClientConfig
                                           NewTopic)
           (org.apache.kafka.clients.consumer ConsumerConfig
                                              ConsumerRecord
                                              ConsumerRecords
                                              KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerConfig
                                              ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.errors InvalidTopicException
                                           TopicExistsException)))

(def port
  "What port do we connect to?"
  9092)

(defn ^Properties ->properties
  "Turns a map into a Properties object."
  [m]
  (doto (Properties.)
    (.putAll (map-vals str m))))

(defn consumer-config
  "Constructs a properties map for talking to a given Kafka node."
  [node]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/consumer/ConsumerConfig.html
  {ConsumerConfig/GROUP_ID_CONFIG
   "clojure_example_group"

   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
   ;"org.apache.kafka.common.serialization.StringDeserializer"
   "org.apache.kafka.common.serialization.LongDeserializer"

   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
   ;"org.apache.kafka.common.serialization.StringDeserializer"
   "org.apache.kafka.common.serialization.LongDeserializer"

   ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG
   (str node ":" port)

   ; Maybe mess with these later?
   ; ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG
   ; 10000

   ; ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG
   ; 10000

   ; ConsumerConfig/ISOLATION_LEVEL_CONFIG
   ; ???

   ; ConsumerConfig/ISOLATION_LEVEL_DOC
   ; ???

   ; ConsumerConfig/DEFAULT_ISOLATION_LEVEL
   ; ???
   })

(defn producer-config
  "Constructs a config map for talking to a given node."
  [node]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/producer/ProducerConfig.html
  {;ProducerConfig/ACKS_CONFIG
   ; ???

   ProducerConfig/BOOTSTRAP_SERVERS_CONFIG
   (str node ":" port)

   ; ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG
   ; ???

   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
   ;"org.apache.kafka.common.serialization.StringSerializer"
   "org.apache.kafka.common.serialization.LongSerializer"

   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
   ;"org.apache.kafka.common.serialization.StringSerializer"
   "org.apache.kafka.common.serialization.LongSerializer"

   ; We want rapid reconnects so we can observe broken-ness
   ProducerConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG
   1000

   ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG
   10000

   ; TODO?
   ; TRANSACTIONAL_ID_CONFIG
   ; ???
   })

(defn admin-config
  "Constructs a config map for an admin client connected to the given node."
  [node]
  ; See https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/admin/AdminClientConfig.html
  {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG
   (str node ":" port)})

(defn consumer
  "Opens a new consumer for the given node."
  [node]
  (KafkaConsumer. (->properties (consumer-config node))))

(defn producer
  "Opens a new producer for a node."
  [node]
  (KafkaProducer. (->properties (producer-config node))))

(defn admin
  "Opens an admin client for a node."
  [node]
  (Admin/create (->properties (admin-config node))))

(defn close!
  "Closes any AutoCloseable."
  [^java.lang.AutoCloseable c]
  (.close c))

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

(defn ^Duration ms->duration
  "Constructs a Duration from millis."
  [ms]
  (Duration/ofMillis ms))

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
