(ns jepsen.redpanda.workload.queue
  "A workload which treats Redpanda more as a queue. Each client maintains a
  producer and consumer. To subscribe to a new set of topics, we issue an
  operation like:

    {:f :subscribe, :value [k1, k2, ...]}

  or

    {:f :assign, :value [k1, k2, ...]}

  ... where k1, k2, etc denote specific topics and partitions. For subscribe,
  we just use that key's topic, and allow Redpanda to control which partitions
  we get. Just like the Kafka client API, both subscribe and assign replace the
  current topics for the consumer.

  Reads and writes (and mixes thereof) are encoded as a vector of
  micro-operations:

    {:f :poll, :value [op1, op2, ...]}
    {:f :send, :value [op1, op2, ...]}
    {:f :txn,  :value [op1, op2, ...]}

  Where :poll and :send denote transactions comprising only reads or writes,
  respectively, and :txn indicates a general-purpose transaction. Operations
  are of two forms:

    [:send key value]

  ... instructs a client to append `value` to the integer `key`--which maps
  uniquely to a single topic and partition. These operations are returned as:

    [:send key [offset value]]

  where offset is the returned offset of the write, if available, or `nil` if
  it is unknown (e.g. if the write times out).

  Reads are invoked as:

    [:poll]

  ... which directs the client to perform a single `poll` operation on its
  consumer. The results of that poll are expanded to:

    [:poll {key1 [[offset1 value1] [offset2 value2] ...],
            key2 [...]}]

  Where key1, key2, etc are integer keys obtained from the topic-partitions
  returned by the call to poll, and the value for that key is a vector of
  [offset value] pairs, corresponding to the offset of that message in that
  particular topic-partition, and the value of the message---presumably,
  whatever was written by `[:send key value]` earlier.

  Before a transaction completes, we commit its offsets.

  From this history we can perform a number of analyses:

  1. For any observed value of a key, we check to make sure that its writer was
  either :ok or :info; if the writer :failed, we know this constitutes an
  aborted read.

  2. We verify that all sends and polls agree on the value for a given key and
  offset. We do not require contiguity in offsets, because transactions add
  invisible messages which take up an offset slot but are not visible to the
  API. If we find divergence, we know that Kakfa disagreed about the value at
  some offset.

  Having verified that each [key offset] pair uniquely identifies a single
  value, we eliminate the offsets altogether and perform the remainder of the
  analysis purely in terms of keys and values. We construct a graph where
  vertices are values, and an edge v1 -> v2 means that v1 immediately precedes
  v2 in the offset order (ignoring gaps in the offsets, which we assume are due
  to transaction metadata messages).

  3. For each key, we take the highest observed offset, and then check that
  every :ok :send operation with an equal or lower offset was *also* read by at
  least one consumer. If we find one, we know a write was lost!

  4. We build a dependency graph between pairs of transactions T1 and T2, where
  T1 != T2, like so:

    ww. T1 sent value v1 to key k, and T2 sent v2 to k, and o1 < o2
        in the version order for k.

    wr. T1 sent v1 to k, and T2's highest read of k was v1.

    rw. T1's highest read of key k was offset o1, and T2 sent offset o2 to k,
        and o1 < o2 in the version order for k.

  Our use of \"highest offset\" is intended to capture the fact that each poll
  operation observes a *range* of offsets, but in general those offsets could
  have been generated by *many* transactions. If we drew wr edges for every
  offset polled, we'd generate superfluous edges--all writers are already
  related via ww dependencies, so the final wr edge, plus those ww edges,
  captures those earlier read values.

  We draw rw edges only for the final versions of each key observed by a
  transaction. If we drew rw edges for an earlier version, we would incorrectly
  be asserting that later transactions were *not* observed!

  We perform cycle detection and categorization of anomalies from this graph
  using Elle.

  5. Internal Read Contiguity: Within a transaction, each pair of reads on the
  same key should be directly related in the version order. If we observe a gap
  (e.g. v1 < ... < v2) that indicates this transaction skipped over some
  values. If we observe an inversion (e.g. v2 < v1, or v2 < ... < v1) then we
  know that the transaction observed an order which disagreed with the \"true\"
  order of the log.

  6. Internal Write Contiguity: Gaps between sequential pairs of writes to the
  same key are detected via Elle as write cycles. Inversions are not, so we
  check for them explicitly: a transaction sends v1, then v2, but v2 < v1 or v2
  < ... v1 in the version order.

  7. Intermediate reads? I assume these happen constantly, but are they
  supposed to? It's not totally clear what this MEANS, but I think it might
  look like a transaction T1 which writes [v1 v2 v3] to k, and another T2 which
  polls k and observes any of v1, v2, or v3, but not *all* of them. This
  miiight be captured as a wr-rw cycle in some cases, but perhaps not all,
  since we're only generating rw edges for final reads."
  (:require [analemma [xml :as xml]
                      [svg :as svg]]
            [bifurcan-clj [core :as b]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ real-pmap loopr]]
            [elle [core :as elle]
                  [graph :as g]
                  [list-append :refer [rand-bg-color]]
                  [txn :as txn]
                  [util :refer [index-of]]
                  [rels :refer [ww wr rw]]]
            [gnuplot.core :as gnuplot]
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
            [knossos [history :as history]]
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

(def default-abort-p
  "What's the probability that we abort a transaction at any given step?"
  1/100)

(def partition-count
  "How many partitions per topic?"
  2)

(def replication-factor
  "What replication factor should we use for each topic?"
  3)

(def poll-ms
  "How long should we poll for, in ms?"
  100)

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

(defn topic-partition->k
  "Turns a TopicPartition into a key."
  ([^TopicPartition tp]
   (topic-partition->k (.topic tp) (.partition tp)))
  ([topic partition]
   (+ (* partition-count (parse-long (nth (re-find #"t(\d+)" topic) 1)))
      partition)))

(defn mop!
  "Applies a micro-operation from a transaction: either a :r read or a :append
  operation."
  [{:keys [extant-topics
           ^Admin admin
           ^KafkaProducer producer
           ^KafkaConsumer consumer] :as client}
   poll-ms
   mop]
  (case (first mop)
    :poll (try
            (rc/unwrap-errors
              (let [records (.poll consumer (rc/ms->duration poll-ms))]
                (->> (.partitions records)
                     (map (fn per-topic-partition [topic-partition]
                            ; Return a key, messages pair
                            [(topic-partition->k topic-partition)
                             ; Turn each message into an offset, record] pair
                             (mapv (fn xform-messages [^ConsumerRecord record]
                                     [(.offset record)
                                      (.value record)])
                                   (.records records topic-partition))]))
                     (into (sorted-map))
                     (vector :poll))))
              (catch InvalidTopicException _
                [:poll {}])
              (catch IllegalStateException e
                (if (re-find #"not subscribed to any" (.getMessage e))
                  [:poll {}]
                  (throw e))))

    :send (let [[f k v]   mop
                topic     (k->topic k)
                ; Create topic if it doesn't exist yet
                _         (when-not (contains? @extant-topics topic)
                            (rc/create-topic! admin topic partition-count
                                              replication-factor)
                            (swap! extant-topics conj topic))
                ; Send message to Redpanda
                partition (k->partition k)
                record (rc/producer-record topic (k->partition k) nil v)
                res    ^RecordMetadata (-> producer
                                           (.send record)
                                           (deref 10000 nil)
                                           (or (throw+ {:type :timeout})))
                k'     (topic-partition->k (.topic res)
                                           (.partition res))
                offset (when (.hasOffset res)
                         (.offset res))]
            (assert+ (= k k')
                     {:type ::key-mismatch
                      :k  k
                      :k' k'})
            ; As it turns out offsets go missing fairly often
            (when-not offset
              (info "Missing offset for send() acknowledgement of key" k "value" v))
            ;(assert+ offset
            ;         {:type ::missing-offset
            ;          :res  res})
            ; And record offset and value.
            [f k' [offset v]])))

(defn send-offsets!
  "Takes a client and a completed txn operation. Finds the highest polled
  offsets from that op's :value atom, and calls .sendOffsetsToTransaction on
  the producer."
  [client op]
  (loopr [offsets {}] ; Map of key to offset.
         [[_ poll]       (->> op :value deref (filter (comp #{:poll} first)))
          [k pairs]      poll
          [offset value] pairs]
         (recur (update offsets k (fnil max ##-Inf) offset))
         ; Convert that to topic-partitions -> OffsetAndMetadata
         (let [kafka-offsets (->> (for [[k offset] offsets]
                                    [(k->topic-partition k)
                                     ; Note that we need to send the *next*
                                     ; offset, not the one we observed.
                                     (rc/offset+metadata (inc offset))])
                                  (into {}))
               producer ^KafkaProducer (:producer client)
               consumer ^KafkaConsumer (:consumer client)]
           (when-not (empty? offsets)
             ;(info :send-offsets offsets)
             (.sendOffsetsToTransaction
               producer
               kafka-offsets
               ; Redpanda doesn't support this version yet; it'll throw:
               ;
               ; org.apache.kafka.common.errors.UnsupportedVersionException:
               ; Broker doesn't support group metadata commit API on version 2,
               ; minimum supported request version is 3 which requires brokers
               ; to be on version 2.5 or above.
               (.groupMetadata consumer))))))
               ; Instead we send the consumer group--this is the old API
               ;rc/consumer-group)))))

(defn safe-abort!
  "Transactional aborts in the Kafka client can themselves fail, which requires
  that we do a complex error-handling dance to retain the original exception as
  well as the one thrown during the abort process. This function takes a
  client, whether we're aborting before or after calling commit, and an
  exception thrown by the transaction body (the reason why we're aborting in
  the first place). Tries to abort the transaction on the current producer.
  Then throws a map of:

    {:type          :abort
     :abort-ok?     True if we successfully aborted, false if the abort threw.
     :tried-commit? Whether we attempted to commit the transaction.
     :body-error    The exception which caused us to try the abort
     :abort-error   If the abort call itself crashed, the exception thrown.
     :definite?     If true, the transaction *should* not have taken place. If
                    false, the transaction may have taken place.}"
  [client tried-commit?, body-error]
  ;(warn body-error "Crashed in txn body")
  (try (rc/abort-txn! (:producer client))
       ; The example we're following for transactional workloads resets the
       ; committed offsets for the consumer on abort. It might *seem* like we
       ; should do this, but it might also create new weird behavior where
       ; the poller jumps over records. Part of the reason they do this in the
       ; EOS demo is because they're using some external consumer group stuff
       ; to ensure there's no concurrent consumers, and we're *not* doing that
       ; here. I'm not sure whether we should include this or not--it doesn't
       ; seem to have a huge effect on safety, so I'm leaving it here for y'all
       ; to consider later.
       ; (rc/reset-to-last-committed-positions! (:consumer client))
       (catch RuntimeException abort-error
         ; But of course the abort can crash! We throw a wrapper exception
         ; which captures both the abort error and the error thrown from the
         ; body that made us *try* the abort.
         ;
         ; When this happens, we need to close the producer.
         (throw+ {:type           :abort
                  :abort-ok?      false
                  :tried-commit?  tried-commit?
                  :definite?
                  (or ; If we didn't try to commit, we know
                      ; this definitely didn't happen.
                      (not tried-commit?)
                      ; Likewise, if we see an invalid txn state during commit,
                      ; that should signal the txn didn't happen.
                      (instance? InvalidTxnStateException body-error))
                  :body-error     body-error
                  :abort-error    abort-error})))
  ; Here we know that the abort completed; it's
  ; safe to re-use this producer.
  (throw+ {:type           :abort
           :abort-ok?      true
           :tried-commit?  tried-commit?
           :definite?      true
           :body-error     body-error}))

(defmacro with-txn
  "Takes a test, a client, an operation, and a body. If (:txn? test) is false,
  evaluates body. If (:txn? test) is true, evaluates body in a transaction:
  beginning the transaction before the body begins, committing the transaction
  once the body completes, and handling errors appropriately.

  Expects the body to produce a completed version of op. Parses op to identify
  the highest observed offset for each poll, and sends those offsets to the
  transaction coordinator.

  If the body throws a fencing/auth/order error, returns a :type :info version
  of op, which forces the client to tear down the producer and create a new
  one.

  TODO: this isn't entirely what we want, because these might be definite
  errors and now we're recording them as indefinite, but it feels safer to
  force a full process crash because a lot of our analysis assumes processes
  are 1:1 with consumers/producers. Maybe later we should close/reopen the
  producer and wrap it in an atom?

  For all other errors, aborts the transaction and throws."
  [test client op & body]
  ; These three specific errors are definite but *cannot* be aborted, so we
  ; have to throw them separately; they'll be handled by with-errors. We also
  ; have to repeat them in two separate try/catch clauses, so we write them
  ; down here.
  (let [definite-non-abortable-catches
        `[(catch ProducerFencedException     e# (throw e#))
          (catch OutOfOrderSequenceException e# (throw e#))
          (catch AuthorizationException      e# (throw e#))]]
    `(if-not (:txn? ~test)
       ; Not a transaction; evaluate normally.
       (do ~@body)
       ; Great, we're a transaction. Let the producer know.
       (let [producer# ^KafkaProducer (:producer ~client)]
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
                           (safe-abort! ~client false body-err#)))]
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
                  (safe-abort! ~client true e#)))
           op'#)))))

(defn serializable-exception
  "Makes an exception safe for representation in our data structures."
  [e]
  (if (instance? clojure.lang.ExceptionInfo e)
    (ex-data e)
    (str e)))

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, catching common exceptions
  and returning appropriate fail/info operations when they occur."
  [op & body]
  `(try+ ~@body
          (catch AuthorizationException _#
            (assoc ~op
                   :type         :fail
                   :error        :authorization
                   :end-process? true))

          (catch DisconnectException e#
           (assoc ~op :type :info, :error [:disconnect (.getMessage e#)]))

          (catch IllegalStateException e#
            (condp re-find (.getMessage e#)
              #"Invalid transition attempted"
              (assoc ~op
                     ; Could be fail, but we need to close the client
                     :type :info
                     :error [:illegal-transition (.getMessage e#)])))

         (catch InvalidProducerEpochException e#
           (assoc ~op
                  :type  :fail
                  :error [:invalid-producer-epoch (.getMessage e#)]))

         (catch InvalidTopicException _#
           (assoc ~op :type :fail, :error :invalid-topic))

         (catch InvalidReplicationFactorException _#
           (assoc ~op :type :fail :error :invalid-replication-factor))

         (catch NetworkException e#
           (assoc ~op :type :info, :error [:network (.getMessage e#)]))

         (catch NotControllerException e#
           (assoc ~op :type :fail, :error :not-controller))

         (catch NotLeaderOrFollowerException _#
           ; This is, surprisingly enough, not a definite failure! See
           ; https://issues.apache.org/jira/browse/KAFKA-13574
           (assoc ~op :type :info, :error :not-leader-or-follower))

         (catch OutOfOrderSequenceException _#
            (assoc ~op
                   :type         :fail
                   :error        :out-of-order-sequence
                   :end-process? true))

         (catch ProducerFencedException _#
            (assoc ~op
                   :type         :fail,
                   :error        :producer-fenced
                   :end-process? true))

         (catch UnknownTopicOrPartitionException _#
           (assoc ~op :type :fail, :error :unknown-topic-or-partition))

         (catch UnknownServerException e#
           (assoc ~op :type :info, :error [:unknown-server-exception
                                          (.getMessage e#)]))

         (catch TimeoutException _#
           (assoc ~op :type :info, :error :kafka-timeout))

         (catch KafkaException e#
           (condp re-find (.getMessage e#)
             #"broker is not available"
             (assoc ~op :type :fail, :error :broker-not-available)

             ; This signifies that the producer is borked somehow and we need
             ; to tear it down and start a new one.
             #"Cannot execute transactional method because we are in an error state"
             (assoc ~op
                    :type         :fail
                    :error        [:txn-in-error-state (.getMessage e#)]
                    :end-process? true)

             #"Unexpected error in AddOffsetsToTxnResponse"
             (assoc ~op :type :fail, :error [:add-offsets (.getMessage e#)])

             #"Unexpected error in TxnOffsetCommitResponse"
             (assoc ~op
                    :type  :fail
                    :error [:txn-offset-commit (.getMessage e#)])

             #"Unhandled error in EndTxnResponse"
             (assoc ~op :type :info, :error [:end-txn (.getMessage e#)])

             (throw e#)))

         (catch [:type :abort] e#
           (assoc ~op
                  :type         (if (:definite? e#) :fail :info)
                  ; If we were able to abort successfully, it's OK to re-use
                  ; this process; otherwise we need to tear it down.
                  :end-process? (not (:abort-ok? e#))
                  :error        (cond-> e#
                                  ; We'd like string representations rather
                                  ; than full objects here, so we can serialize
                                  ; the history.
                                  true
                                  (update :body-error serializable-exception)

                                  (:abort-error e#)
                                  (update :abort-error serializable-exception))))

         (catch [:type :partitions-assigned] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :partitions-lost] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :partitions-revoked] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :timeout] e#
           (assoc ~op :type :info, :error :timeout))))

(defmacro with-rebalance-log
  "Ugh, everything needs state tracking. I'm so sorry, this is an enormous
  spaghetti pile.

  Basically, we want to know whether rebalances happened during a given
  transaction, because we suspect that they might be implicated in certain
  Weird Polling Events. We take a client and a body. Before the body, we clear
  the client's rebalance log atom, and when the body is complete (returning
  op') we snarf the client's rebalance log and store it in the completion op,
  so that it's available for inspection later."
  [client & body]
  `(let [log# (:rebalance-log ~client)]
     (reset! log# [])
     (let [op'# ~@body
           log# (map (fn [entry#]
                       (-> entry#
                           (dissoc :partitions)
                           (assoc :keys (map topic-partition->k
                                             (:partitions entry#)))))
                     @log#)]
       (if (seq log#)
         (assoc op'# :rebalance-log log#)
         op'#))))

(defmacro with-mutable-value
  "Takes a symbol referring to an invocation operation, and evaluates body
  where `op` has a :value which is an atom wrapping the original op's :value.
  Expects body to return an op, and replaces that op's :value with the current
  value of the atom.

  We do this Rather Weird Thing because Redpanda might actually give us
  information like offsets for `send` during a transaction which will later
  crash, and we want to preserve those offsets in the completed info op."
  [op & body]
  (assert (symbol? op))
  `(let [value# (atom (:value ~op))
         ~op    (assoc ~op :value value#)
         op'#   (do ~@body)]
     (assoc op'# :value @value#)))

(defn maybe-abort
  "Kafka transaction recovery is... weird. We intentionally stress it by
  throwing exceptions inside transactions, with probability (:abort-p test), at
  each step of transaction execution."
  [test]
  (when (and (:txn? test)
             (< (rand) (:abort-p test default-abort-p)))
    (throw+ {:type :intentional-abort})))

(defrecord Client [; What node are we bound to?
                   node
                   ; Our three Kafka clients
                   ^Admin admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   ; An atom with a set of topics we've created. We have to
                   ; create topics before they can be used.
                   extant-topics
                   ; An atom of a vector of consumer rebalance events
                   ; love too track state everywhere
                   rebalance-log]
  client/Client
  (open! [this test node]
    (let [tx-id    (rc/new-transactional-id)
          producer-opts (assoc test :client-id (str "jepsen-" tx-id))
          producer (rc/producer
                     (if (:txn? test)
                       (assoc test :transactional-id tx-id)
                       test)
                     node)]
      (info "transactional-id" tx-id)
      (assoc this
             :node          node
             :admin         (rc/admin test node)
             :consumer      (rc/consumer test node)
             :producer      producer
             :rebalance-log (atom []))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      ; Assign this consumer new topic-partitions
      :assign (let [tps (map k->topic-partition (:value op))]
                (.assign consumer tps)
                (when (:seek-to-beginning? op)
                  (info "Seeking to beginning")
                  (.seekToBeginning consumer tps))
                (assoc op :type :ok))

      ; Crash this client, forcing us to open a new client (and consumer etc)
      :crash     (assoc op :type :info)

      ; Get some debugging information about the partition distribution on this
      ; node
      :debug-topic-partitions
      (try+
        (try
          (let [tps (->> (:value op)
                         (real-pmap (fn [k]
                                 [k (db/topic-partition-state (:db test)
                                      node (k->topic-partition k))]))
                         (into (sorted-map)))]
            (assoc op :type :ok, :value {:node    node
                                         :node-id (db/node-id (:db test)
                                                              test node)
                                         :partitions tps}))
          (catch java.util.concurrent.ExecutionException e
            (throw (util/ex-root-cause e))))
        (catch [:type :clj-http.client/unexceptional-status] e
          (assoc op :type :fail, :error (:body e)))
        (catch java.net.SocketTimeoutException _
          (assoc op :type :fail, :error :timeout))
        (catch java.net.ConnectException _
          (assoc op :type :fail, :error :connection-refused)))

      ; Subscribe to the topics containing these keys
      :subscribe
      (let [topics (->> (:value op)
                        (map k->topic)
                        distinct)]
        (if (:txn? test)
          (rc/subscribe! consumer
                         topics
                         (rc/logging-rebalance-listener rebalance-log))
          (rc/subscribe! consumer topics))
        (assoc op :type :ok))

      ; Apply poll/send transactions
      (:poll, :send, :txn)
      (with-mutable-value op
        (with-rebalance-log this
          (with-errors op
            (with-txn test this op
              (rc/unwrap-errors
                (do ; Evaluate micro-ops for side effects, incrementally
                    ; transforming the transaction's micro-ops
                    (reduce (fn [i mop]
                              (maybe-abort test)
                              (let [mop' (mop! this (:poll-ms op poll-ms) mop)]
                                (swap! (:value op) assoc i mop'))
                              (inc i))
                            0
                            @(:value op))
                    ; Final chance to abort before committing
                    (maybe-abort test)
                    ; If we read, AND we're using :subscribe instead of assign,
                    ; commit offsets. My understanding is that with assign
                    ; you're not supposed to use the offset system?
                    ;
                    ; Also note that per
                    ; https://kafka.apache.org/30/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#sendOffsetsToTransaction(java.util.Map,org.apache.kafka.clients.consumer.ConsumerGroupMetadata), we shouldn't use commit manually; instead we let sendOffsetsToTransaction handle the commit.
                    (when (and (#{:poll :txn} (:f op))
                               (not (:txn? test))
                               (:subscribe (:sub-via test)))
                      (try (.commitSync consumer)
                           ; If we crash during commitSync *outside* a
                           ; transaction, it might be that we poll()ed some
                           ; values in this txn which Kafka will think we
                           ; consumed. We won't have any record of them if we
                           ; fail the txn. Instead, we return an :ok txn *with*
                           ; the reads, but note the lack of commit.
                           (catch RuntimeException e
                             (assoc op :type :ok
                                    :error [:consumer-commit (.getMessage e)]))))
                    (assoc op :type :ok)))))))))

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
  (map->Client {:extant-topics (atom #{})}))

(defn op->max-offsets
  "Takes an operation (presumably, an OK one) and returns a map of keys to the
  highest offsets interacted with in that op."
  [{:keys [type f value]}]
  (case type
    (:info, :ok)
    (case f
      (:poll, :send, :txn)
      (->> value
           (map (fn [[f :as mop]]
                  (case f
                    :poll (->> (second mop)
                               (map-vals (fn [pairs]
                                           (->> pairs
                                                (map first)
                                                (remove nil?)
                                                (reduce max -1)))))
                    :send (let [[_ k v] mop]
                            (when (and (vector? v) (first v))
                              {k (first v)})))))
           (reduce (partial merge-with max)))

      nil)
    nil))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map. Options are:

    :crash-clients? If set, periodically emits a :crash operation which the
                    client responds to with :info; this forces the client to be
                    torn down and replaced by a fresh client.

    :crash-client-interval How often, in seconds, to crash clients. Default is
                           30 seconds.

    :sub-via        A set of subscription methods: either #{:assign} or
                    #{:subscribe}.

    :txn?           If set, generates transactions with multiple send/poll
                    micro-operations.

  These options must also be present in the test map, because they are used by
  the checker, client, etc at various points. For your convenience, they are
  included in the workload map returned from this function; merging that map
  into your test should do the trick.

  ... plus those taken by jepsen.tests.cycle.append/test, e.g. :key-count,
  :min-txn-length, ..."
  [opts]
  (let [workload (jepsen.tests.kafka/workload opts)]
    (assoc workload :client (client))))
