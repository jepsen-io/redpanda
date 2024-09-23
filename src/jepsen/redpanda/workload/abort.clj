(ns jepsen.redpanda.workload.abort
  "What happens when a transaction aborts? Do consumers rewind, or do they keep
  going?

  With a single producer and a single consumer, we...

  1. Create a topic with 1 partition and write a few records to it. Here
  we create topic t1:

    {:type :invoke, :f :create, :value {:topic \"t1\" :count 128}
    {:type :ok,     :f :create, :value {:topic \"t1\" :count 128}}

  2. We subscribe or assign (depending on (:sub-via test)) to the topic.

    {:type :invoke, :f :subscribe, :value \"t1\"}
    {:type :ok,     :f :subscribe, :value \"t1\"}

  3. We start a transaction, poll once, and commit. We do this until the
     transaction receives some records.

    {:type :invoke, :f :poll, :value nil
    {:type :ok,     :f :poll, :value {:topic \"t1\", :offsets [1 2 3...]}}

  4. As soon as there are records received, we repeat the poll
     transaction, but abort it instead of committing. We do this, similarly,
     until it receives at least some records. We repeat this a few times.

      {:type :invoke, :f :poll, :value {:abort? true}
      {:type :fail,   :f :poll, :value {:topic \"t1\", :abort? true,
                                        :offsets [1 2 3...]}}

  5. We do one final poll, this time committing. Again, we repeat this until it
  gets some records.

      {:type :invoke, :f :poll, :value nil
      {:type :ok,     :f :poll, :value {:topic \"t1\", :offsets [1 2 3...]}}

  To check this topic we examine the polled offsets in each transaction, and
  consider every poll-abort in the context of its next and previous operations.
  We classify each poll-abort as:

    - :rewind:          The consumer picked up where the failed txn began
    - :rewind-further   The consumer picked up *earlier* than that
    - :unchanged        The consumer picked up after the failed txn"
  (:require [bifurcan-clj [core :as b]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ real-pmap loopr]]
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

(defn send-offsets!
  "Takes a client and a completed poll operation. Calls
  .sendOffsetsToTransaction on the producer, based on the op's :value atom."
  [client op]
  ; As a reminder, our ops have a structure like
  ; {:type :ok, :f :poll, :value {:topic \"t1\", :offsets [1 2 3...]}}
  (let [value @(:value op)
        topic (:topic value)
        offset (reduce max -1 (:offsets value))]
    (when (< -1 offset)
      ; Remember to add one! Because the max committed offset is, in fact, not
      ; a committed offset!
      (let [kafka-offset (rc/offset+metadata (inc offset))
            producer ^KafkaProducer (:producer client)
            consumer ^KafkaConsumer (:consumer client)]
        (.sendOffsetsToTransaction producer {topic kafka-offset})
        (.groupMetadata consumer)))))

(defmacro with-txn
  "Takes a client, an operation, and a body. Evaluates body in a transaction.
  Expects body to produce a completed version of `op`. See rq/with-txn for
  details."
  [client op & body]
  ; These three specific errors are definite but *cannot* be aborted, so we
  ; have to throw them separately; they'll be handled by with-errors. We also
  ; have to repeat them in two separate try/catch clauses, so we write them
  ; down here.
  (let [definite-non-abortable-catches
        `[(catch ProducerFencedException     e# (throw e#))
          (catch OutOfOrderSequenceException e# (throw e#))
          (catch AuthorizationException      e# (throw e#))]]
    `(let [producer# ^KafkaProducer (:producer ~client)]
       (.beginTransaction producer#)
       ; Actually evaluate body, and possibly send offsets to the txn
       (let [op'# (try (let [op'# (do ~@body)]
                         (send-offsets! ~client op'#)
                         op'#)
                       ; Some errors aren't allowed to abort
                       ~@definite-non-abortable-catches
                       ; For all other errors...
                       (catch RuntimeException body-err#
                         ; If we crash *prior* to commit, we're allowed to
                         ; abort
                         (rq/safe-abort! ~client false body-err#)))]
         ; Now we can commit the txn
         (try (.commitTransaction producer#)
              ; Again, some errors aren't allowed to abort
              ~@definite-non-abortable-catches
              ; Timeouts and interrupts are indefinite and also
              ; non-abortable; we throw these and let the enclosing error
              ; handler grab them.
              (catch TimeoutException e#
                (throw e#))
              (catch InterruptException e#
                (throw e#))
              ; But for *other* exceptions, we need to abort, and of
              ; course that can fail again as well, so we call safe-abort!
              (catch RuntimeException e#
                (rq/safe-abort! ~client true e#)))
         op'#))))

(defn send!
  "Sends a message to a topic's sole partition, returning a promise of an
  offset."
  [^KafkaProducer producer, topic, message]
  (let [fut (.send producer
                   (rc/producer-record topic 0 nil message))]
    (delay
      (let [res ^RecordMetadata (-> fut
                                    (deref 1000 nil)
                                    (or (throw+ {:type :timeout})))]
        (when (.hasOffset res)
          (.offset res))))))

(defn drain-rebalance-log!
  "Takes a rebalance log atom, and an atom to drain it to. Swaps the rebalance
  log with an empty vector, storing its contents in the drain atom."
  [from to]
  (swap! from (fn swap [log]
                (reset! to log)
                [])))

(defmacro with-rebalance-log
  "Ugh, everything needs state tracking. I'm so sorry, this is an enormous
  spaghetti pile.

  Basically, we want to know whether rebalances happened during a given
  transaction, because we suspect that they might be implicated in certain
  Weird Polling Events. We take a client and a body. We drain the rebalance
  log, evaluate the body, then drain the log again. We store these in
  (:rebalance-log op) under two sub-keys:

      {:before [...]
       :during [...]}"
  [client & body]
  `(let [before# (atom [])
         during# (atom [])
         log#    (:rebalance-log ~client)]
     (drain-rebalance-log! log# before#)
     (let [op'# ~@body]
       (drain-rebalance-log! log# during#)
       (if (or (seq @before#) (seq @during#))
         (assoc op'# :rebalance-log
                {:before @before#
                 :during @during#})
         op'#))))

(defn random-chunks
  "Randomly chunk a seq into small batches."
  [xs]
  (when (seq xs)
    (lazy-seq
      (let [[chunk more] (split-at (rand-int 6) xs)]
        (cons chunk (random-chunks more))))))

(defrecord Client [; What node are we bound to?
                   node
                   ; Our three Kafka clients
                   ^Admin admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   ; An atom of a vector of consumer rebalance events
                   ; love too track state everywhere
                   rebalance-log]
  client/Client
  (open! [this test node]
    (let [tx-id         (rc/new-transactional-id)
          producer-opts (assoc test :client-id (str "jepsen-" tx-id))
          producer      (rc/producer
                          (assoc test :transactional-id tx-id)
                          node)]
      (info "transactional-id" tx-id)
      (assoc this
             :node          node
             :admin         (rc/admin test node)
             ; We intentionally want small polls
             :consumer      (rc/consumer (assoc test
                                                :max-poll-records 4)
                                         node)
             :producer      producer
             :rebalance-log (atom []))))

  (setup! [this test])

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      ; Create a topic
      :create
      (let [topic (:topic value)]
        (rc/create-topic! admin topic 1 rq/replication-factor)
        (assoc op :type :ok))

      ; Assign this consumer to a topic
      :assign (do (.assign consumer [(rc/topic-partition value 0)])
                  (assoc op :type :ok))

      ; Subscribe to a topic
      :subscribe (do (rc/subscribe!
                       consumer
                       [value]
                       (rc/logging-rebalance-listener rebalance-log))
                     (assoc op :type :ok))

      ; Crash this client, forcing us to open a new client (and consumer etc)
      :crash     (assoc op :type :info)

      ; A poll transaction
      :poll
      ; We have several places to introduce an abort; pick one at random
      (let [abort-at (if (:abort? value)
                       (rand-int 3)
                       -1)]
        (rq/with-mutable-value op
          (with-rebalance-log this
            (rq/with-errors op
              (rq/with-txn {:txn? true} this op
                (rc/unwrap-errors
                  ; Send a few messages
                  ;(dotimes [i (rand-int 3)]
                  ;  (send! producer (:topic value) -1))
                  ;(when (= abort-at 0)
                  ;  (throw+ {:type :intentional-abort}))
                  ; Poll
                  (let [records (.poll consumer (rc/ms->duration rq/poll-ms))]
                    (condp = (count (.partitions records))
                      0 nil
                      1 (let [tp (first (.partitions records))
                              topic (.topic tp)
                              offsets (mapv (fn [^ConsumerRecord record]
                                              (.offset record))
                                            (.records records tp))]
                          ; Note that our value here is mutable; we do this so
                          ; we can preserve state even when throwing
                          (swap! (:value op) assoc
                                 :topic topic
                                 :offsets offsets))
                      (throw+ {:type :too-many-partitions
                               :partitons (.partitions records)}))
                    ; Possible abort
                    (when (= abort-at 1)
                      (throw+ {:type :intentional-abort}))
                    ; Send a few messages
                    (dotimes [i (rand-int 4)]
                      (send! producer (:topic value) -1))
                    ; Possible abort
                    (when (= abort-at 2)
                      (throw+ {:type :intentional-abort}))
                    (assoc op :type :ok))))))))))

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

(defn until-polled
  "Wraps a generator in one which stops when (:offsets (:value op)) is
  non-empty."
  [gen]
  (reify gen/Generator
    (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test ctx)]
        (if (= :pending op)
          [:pending this]
          [op (until-polled gen')])))

    (update [this test ctx event]
      (if (and (not= :invoke (:type event))
               (= :poll (:f event))
               (seq (:offsets (:value event))))
        ; Done
        nil
        ; Keep going
        (until-polled (gen/update gen test ctx event))))))

(defn topic-generator
  "Constructs a generator for a single topic"
  [next-topic]
  (fn [test ctx]
    (let [topic (str "t" (swap! next-topic inc))]
      [; Create topic
       {:f :create, :value {:topic topic, :count 128}}
       ; Sub to it
       {:f (rand-nth (vec (:sub-via test))), :value topic}
       ; One OK poll
       (until-polled (gen/repeat {:f :poll, :value {:topic topic}}))
       ; Two aborted polls
       (gen/cycle 2 (until-polled
                      (gen/repeat {:f :poll, :value {:topic topic
                                                     :abort? true}})))
       ; One OK poll
       (until-polled (gen/repeat {:f :poll, :value {:topic topic}}))])))

(defn generator
  "Repeatedly runs the topic generator, constrained to single threads."
  []
  (let [next-topic (atom 0)]
    (gen/each-process
      (topic-generator next-topic))))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (loopr [; A map of process to process-local state as we reduce through
              ; the history
              ;
              ; :prev-topic         The topic this process is working with
              ; :prev-op            The last completed op by this process
              ; :prev-first-offset  The first offset from prev op
              ; :prev-last-offset   The last offset of the prev op
              ; :prev-rebalance?    Has a rebalance occurred since (but not
              ;                     during) the prev op?
              by-process {}
              ; Vector of delta maps
              deltas []]
             ; Loop through polls
             [{:keys [process value] :as op}
              (->> history
                   (h/remove h/invoke?)
                   (h/filter (h/has-f? :poll)))]
               (let [topic        (:topic value)
                     first-offset (first (:offsets value))
                     last-offset  (peek (:offsets value))
                     rebalance?   (boolean (seq (:rebalance-log op)))
                     state'       {:prev-topic        topic
                                   :prev-op           op
                                   :prev-first-offset first-offset
                                   :prev-last-offset  last-offset}
                     by-process' (assoc by-process process state')]
                 (if first-offset
                   (if-let [{:keys [prev-topic
                                    prev-op
                                    prev-first-offset
                                    prev-last-offset
                                    prev-rebalance?]}
                            (get by-process process)]
                     (if (not= prev-topic topic)
                       ; Fresh topic
                       (recur by-process' deltas)
                       ; Same topic, compute deltas
                       (let [type (cond
                                    (< prev-last-offset first-offset) :advance
                                    (= prev-first-offset first-offset) :rewind

                                    (< first-offset prev-first-offset)
                                    :rewind-further

                                    true :other)
                             delta
                             {:prev-offsets (:offsets (:value prev-op))
                              :offsets      (:offsets (:value op))
                              :type         type
                              :rebalance    (if (or rebalance? prev-rebalance?)
                                              :rebalance
                                              :none)
                              :ops          [prev-op op]}]
                         (recur by-process' (conj deltas delta))))
                     ; Fresh process
                     (recur by-process' deltas))
                 ; Nothing in this poll; no offsets to change, but we might
                 ; have a rebalance
                 (recur (update by-process process
                                update :prev-rebalance? #(or % rebalance?))
                        deltas)))
             ; Done iterating
             (let [groups (group-by (juxt :type :rebalance) deltas)]
               (merge {:valid?  (every? #{[:rewind true]
                                          [:rewind false]}
                                        (keys groups))
                       :counts  (update-vals groups count)}
                      (update-vals groups (partial take 4))))))))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map. Options are:

    :sub-via        A set of subscription methods: either #{:assign} or
                    #{:subscribe}.

  These options must also be present in the test map, because they are used by
  the checker, client, etc at various points. For your convenience, they are
  included in the workload map returned from this function; merging that map
  into your test should do the trick."
  [opts]
  (merge opts
         {:generator (generator)
          :client    (client)
          :checker   (checker)}))
