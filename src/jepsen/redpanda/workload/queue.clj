(ns jepsen.redpanda.workload.queue
  "A workload which treats Redpanda more as a queue. Each client maintains a
  producer and consumer. To subscribe to a new set of topics, we issue an
  operation like:

    {:f :subscribe, :value [topic1, topic2, ...]}

  Just like the Kafka client API, this replaces the current topics subscribed
  to.

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

  6. Intermediate reads? I assume these happen constantly, but are they
  supposed to? It's not totally clear what this MEANS, but I think it might
  look like a transaction T1 which writes [v1 v2 v3] to k, and another T2 which
  polls k and observes any of v1, v2, or v3, but not *all* of them. This
  miiight be captured as a wr-rw cycle in some cases, but perhaps not all,
  since we're only generating rw edges for final reads."
  (:require [clojure [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util :refer [map-vals parse-long pprint-str]]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.redpanda [client :as rc]])
  (:import (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin Admin)
           (org.apache.kafka.clients.consumer ConsumerRecords
                                              ConsumerRecord
                                              KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer
                                              RecordMetadata)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.errors InvalidTopicException
                                           NotLeaderOrFollowerException
                                           TimeoutException
                                           UnknownTopicOrPartitionException
                                           UnknownServerException
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

(defn topic-partition->k
  "Turns a TopicPartition into a key."
  ([^TopicPartition tp]
   (topic-partition->k (.topic tp) (.partition tp)))
  ([topic partition]
   (+ (* partition-count (parse-long (nth (re-find #"t(\d+)" topic) 1)))
      partition)))

(def replication-factor
  "What replication factor should we use for each topic?"
  3)

(defn mop!
  "Applies a micro-operation from a transaction: either a :r read or a :append
  operation."
  [{:keys [extant-topics
           ^Admin admin
           ^KafkaProducer producer
           ^KafkaConsumer consumer] :as client}
   mop]
  (case (first mop)
    :poll (try
            (rc/unwrap-errors
              (let [records (.poll consumer (rc/ms->duration 10))]
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
                [:poll []])
              (catch IllegalStateException e
                (if (re-find #"not subscribed to any" (.getMessage e))
                  [:poll []]
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
                res    ^RecordMetadata @(.send producer record)
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

(defrecord Client [; Our three Kafka clients
                   ^Admin admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   ; An atom with a set of topics we've created. We have to
                   ; create topics before they can be used.
                   extant-topics]
  client/Client
  (open! [this test node]
    (let [tx-id (rc/new-transactional-id)]
      (assoc this
             :admin     (rc/admin test node)
;             :producer  (rc/producer (assoc test :transactional-id tx-id) node)
             :producer  (rc/producer test node)
             :consumer  (rc/consumer test node))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :subscribe (do (->> (:value op)
                          (map k->topic)
                          distinct
                          (rc/subscribe! consumer))
                     (assoc op :type :ok))

      (:poll, :send, :txn)
      (let [; How should we interpret the operation completion :type when a
            ; single micro-op definitely fails?
            mop-fail-type (if (= 1 (count (:value op)))
                            ; If we only tried a single mop, we can just fail
                            ; the whole op.
                            :fail
                            ; TODO: when we do transactions, the failure of any
                            ; single mop can be safely interpreted as a :fail
                            ; of the entire op, since all mops should fail to
                            ; commit as a unit. Without txns, these should be
                            ; info, because earlier writes may have committed.
                            :info)]
        (try
          (rc/unwrap-errors
            ;(.beginTransaction producer)
            (let [txn  (:value op)
                  txn' (mapv (partial mop! this) txn)]
              ; If we read, commit offsets.
              (when (#{:poll :txn} (:f op))
                (.commitSync consumer))

              ; TODO: enable txns, write some kind of macro for commit/abort.
              ;(.commitTransaction producer)
              ;(.abortTransaction producer)
              (assoc op :type :ok, :value txn')))
          (catch InvalidTopicException _
            (assoc op :type mop-fail-type, :error :invalid-topic))

          (catch NotLeaderOrFollowerException _
            (assoc op :type mop-fail-type, :error :not-leader-or-follower))

          (catch UnknownTopicOrPartitionException _
            (assoc op :type mop-fail-type, :error :unknown-topic-or-partition))

          (catch UnknownServerException e
            (assoc op :type :info, :error [:unknown-server-exception
                                           (.getMessage e)]))

          (catch TimeoutException _
            (assoc op :type :info, :error :timeout))))))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (rc/close! producer)
    (rc/close! consumer)))

(defn client
  "Constructs a fresh client for this workload."
  []
  (map->Client {:extant-topics (atom #{})}))

(defn txn-generator
  "Takes a list-append generator and rewrites its transactions to be [:poll] or
  [:send k v] micro-ops. Also adds a :keys field onto each operation, with a
  set of keys that txn would have interacted with; we use this to generate
  :subscribe ops later."
  [la-gen]
  (gen/map (fn rewrite-op [op]
             (-> op
                 (assoc :keys (set (map second (:value op))))
                 (update :value
                         (partial mapv (fn rewrite-mop [[f k v]]
                                         (case f
                                           :append [:send k v]
                                           :r      [:poll]))))))
           la-gen))

(def subscribe-ratio
  "How many subscribe ops should we issue per txn op?"
  1/8)

(defrecord InterleaveSubscribes [gen]
  gen/Generator
  (op [this test context]
    ; When we're asked for an operation, ask the underlying generator for
    ; one...
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        (let [this' (InterleaveSubscribes. gen')
              op'   (dissoc op :keys)]
          (if (< (rand) subscribe-ratio)
            ; At random, emit a subscribe op instead.
            [(gen/fill-in-op {:f :subscribe, :value (vec (:keys op))} context)
             this]
            ; Or pass through the op directly
            [op' (InterleaveSubscribes. gen')])))))

  ; Pass through updates
  (update [this test context event]
    (InterleaveSubscribes. (gen/update gen test context event))))

(defn interleave-subscribes
  "Takes a txn generator and keeps track of the keys flowing through it,
  interspersing occasional :subscribe operations for recently seen keys."
  [txn-gen]
  (InterleaveSubscribes. txn-gen))

(defn tag-rw
  "Takes a generator and tags operations as :f :poll or :send if they're
  entirely comprised of send/polls."
  [gen]
  (gen/map (fn tag-rw [op]
             (case (->> op :value (map first) set)
               #{:poll}  (assoc op :f :poll)
               #{:send}  (assoc op :f :send)
               op))
           gen))

;; Checker

(defn assocv
  "An assoc on vectors which allows you to assoc at arbitrary indexes, growing
  the vector as needed. When v is nil, constructs a fresh vector rather than a
  map."
  [v i value]
  (if v
    (if (<= i (count v))
      (assoc v i value)
      (let [nils (repeat (- i (count v)) nil)]
        (assoc (into v nils) i value)))
    ; Nil is treated as an empty vector.
    (recur [] i value)))

(defn nth+
  "Nth for vectors, but returns nil instead of out-of-bounds."
  [v i]
  (when (< i (count v))
    (nth v i)))

(defn version-orders-update-log
  "Updates a version orders log with the given offset and value."
  [log offset value]
  (if-let [values (nth+ log offset)]
    (assoc  log offset (conj values value)) ; Already have values
    (assocv log offset #{value}))) ; First time we've seen this offset

(defn version-orders-reduce-mop
  "Takes a logs object from version-orders and a micro-op, and integrates that
  micro-op's information about offsets into the logs."
  [logs mop]
  (case (first mop)
    :send (let [[_ k v] mop]
            (if (vector? v)
              (let [[offset value] v]
                (if offset
                  ; We completed the send and know an offset
                  (update logs k version-orders-update-log offset value)
                  ; Not sure what the offset was
                  logs))
              ; Not even offset structure: maybe an :info txn
              logs))

    :poll (reduce (fn poll-key [logs [k pairs]]
                    (reduce (fn pair [logs [offset value]]
                              (if offset
                                (update logs k version-orders-update-log
                                        offset value)
                                logs))
                            logs
                            pairs))
                  logs
                  (second mop))))

(defn index-seq
  "Takes a seq of distinct values, and returns a map of:

    {:by-index    A vector of the sequence
     :by-value    A map of values to their indices in the vector.}"
  [xs]
  {:by-index (vec xs)
   :by-value (into {} (map-indexed (fn [i x] [x i]) xs))})

(defn version-orders
  "Takes a history and constructs a map of:

  {:orders   A map of keys to orders for that key. Each order is a map of:
               {:by-index  A vector of all values in log order.
                :by-value  A map of values to indexes in the log.}

   :errors   A series of error maps describing any incompatible orders, where
             a single offset for a key maps to multiple values.}"
  ([history]
   (version-orders history {}))
  ([history logs]
   ; Logs is a map of keys to vectors, where index i in one of those vectors is
   ; the vector of all observed values for that index.
   (if (seq history)
     (let [op       (first history)
           history' (next history)]
       (case (:type op)
         ; There's nothing we can tell from invokes or fails
         (:invoke, :fail) (recur history' logs)

         ; Infos... right now nothing but we MIGHT be able to get partial info
         ; later, so we'll process them anyway.
         (case (:f op)
           :subscribe (recur history' logs)

           (:poll :send :txn) (recur history' (reduce version-orders-reduce-mop
                                                      logs
                                                      (:value op))))))
     ; All done; transform our logs to orders.
     {:errors (->> logs
                   (mapcat (fn errors [[k log]]
                             (->> log
                                  (map-indexed (fn per-key [offset values]
                                                 (when (< 1 (count values))
                                                   {:key    k
                                                    :offset offset
                                                    :values values})))
                                  (remove nil?))))
                   seq)
      :orders (map-vals (comp index-seq (partial keep first)) logs)})))

(defn op-writes
  "Returns a map of keys to the sequence of all values written in an op."
  [op]
  (reduce (fn [writes mop]
            (if (= :send (first mop))
              (let [[_ k v] mop
                    vs (get writes k [])
                    ; Values can be either a literal value or a [offset value]
                    ; pair.
                    value (if (vector? v) (second v) v)]
                (assoc writes k (conj vs value)))
              ; Something other than a send
              writes))
          {}
          (:value op)))

(defn op-reads
  "Returns a map of keys to the sequence of all values read by an op for that
  key."
  [op]
  (reduce (fn mop [writes mop]
            (if (= :poll (first mop))
              (reduce (fn per-key [writes [k pairs]]
                        (let [vs  (get writes k [])
                              vs' (into vs (map second pairs))]
                          (assoc writes k vs')))
                      writes
                      (second mop))
              writes))
          {}
          (:value op)))

(defn reads-of-key
  "Returns a seq of all operations which read the given key."
  [k history]
  (->> history
       (filter (comp #{:txn :send :poll} :f))
       (filter (fn [op]
                 (contains? (op-reads op) k)))))

(defn writes-of-key
  "Returns a seq of all operations which wrote the given key."
  [k history]
  (->> history
       (filter (comp #{:txn :send :poll} :f))
       (filter (fn [op]
                 (contains? (op-writes op) k)))))

(defn writes-by-type
  "Takes a history and constructs a map of types (:ok, :info, :fail) to maps of
  keys to the set of all values which were written for that key. We use this to
  identify, for instance, what all the known-failed writes were for a given
  key."
  [history]
  (->> history
       (remove (comp #{:invoke} :type))
       (filter (comp #{:txn :send} :f))
       (group-by :type)
       (map-vals (fn [ops]
                   (->> ops
                        ; Construct a seq of {key [v1 v2 ...]} maps
                        (map op-writes)
                        ; And turn [v1 v2 ...] into #{v1 v2 ...}
                        (map (partial map-vals set))
                        ; Then merge them all together
                        (reduce (partial merge-with set/union) {}))))))

(defn reads-by-type
  "Takes a history and constructs a map of types (:ok, :info, :fail) to maps of
  keys to the set of all values which were read for that key. We use this to
  identify, for instance, the known-successful reads for some key as a part of
  finding lost updates."
  [history]
  (->> history
       (remove (comp #{:invoke} :type))
       (filter (comp #{:txn :poll} :f))
       (group-by :type)
       (map-vals (fn [ops]
                   (->> ops
                        (map op-reads)
                        (map (partial map-vals set))
                        (reduce (partial merge-with set/union) {}))))))

(defn g1a-cases
  "Takes a partial analysis and looks for aborted reads, where a known-failed
  write is nonetheless visible to a committed read. Returns a seq of error
  maps, or nil if none are found."
  [{:keys [history writes-by-type]}]
  (let [failed (:fail writes-by-type)
        ops (->> history
                 (filter (comp #{:ok} :type))
                 (filter (comp #{:txn :poll} :f)))]
    (->> (for [op     ops
               [k vs] (op-reads op)
               v      vs
               :when (contains? (get failed k) v)]
           {:op    op
            :key   k
            :value v})
         seq)))

(defn lost-update-cases
  "Takes a partial analysis and looks for cases of lost update: where a write
  that we *should* have observed is somehow not observed. Of course we cannot
  expect to observe everything: for example, if we send a message to Redpanda
  at the end of a test, and don't poll for it, there's no chance of us seeing
  it at all! Or a poller could fall behind.

  What we do instead is identify the highest read value for each key v_max, and
  then take the set of all values *prior* to it in the version order: surely,
  if we read v_max = 3, and the version order is [1 2 3 4], we should also have
  read 1 and 2.

  Once we've derived the set of values we ought to have read for some key k, we
  run through each poll of k and cross off the values read. If there are any
  values left, they must be lost updates."
  [{:keys [history version-orders reads-by-type]}]
  ; Start with all the values we know were read
  (->> (:ok reads-by-type)
       (keep (fn [[k vs]]
              ; Great, now for this key, find the highest index observed
              (let [vo         (get version-orders k)
                    _ (info :k k :vs vs :vo vo)
                    last-index (->> vs
                                    ; We might observe a value but *not* know
                                    ; its offset from either write or read.
                                    ; When this happens, we can't say anything
                                    ; about how much of the partition should
                                    ; have been observed, so we skip it.
                                    (keep (:by-value vo))
                                    (reduce max -1))
                    ; Now take a prefix of the version order up to that index;
                    ; these are the values we should have observed.
                    must-read (subvec (:by-index vo) 0 (inc last-index))
                    ; Now we go *back* to the read values vs, and strip them
                    ; out from the must-read vector; anything left is something
                    ; we failed to read.
                    lost (remove vs must-read)]
                (when (seq lost)
                  {:key  k
                   :lost lost}))))
       seq))

(defn analysis
  "Builds up intermediate data structures used to understand a history."
  [history]
  (let [history               (remove (comp #{:nemesis} :process) history)
        version-orders        (version-orders history)
        version-order-errors  (:errors version-orders)
        version-orders        (:orders version-orders)
        writes-by-type        (writes-by-type history)
        reads-by-type         (reads-by-type history)
        analysis              {:history        history
                               :writes-by-type writes-by-type
                               :reads-by-type  reads-by-type
                               :version-orders version-orders}
        g1a-cases             (g1a-cases analysis)
        lost-update-cases     (lost-update-cases analysis)
        ]
    {:errors (cond-> {}
               version-order-errors
               (assoc :inconsistent-offsets version-order-errors)

               g1a-cases
               (assoc :g1a g1a-cases)

               lost-update-cases
               (assoc :lost-update lost-update-cases))
     :version-orders version-orders}))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [analysis (analysis history)]
        {:valid?          (empty? (:errors analysis))
         :errors          (:errors analysis)
         :version-orders  (:version-orders analysis)}))))

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
                          :max-txn-length 4
                          :consistency-models [:strict-serializable]))]
    (-> workload
        (assoc :client  (client)
               :checker (checker))
        (update :generator
                (fn wrap-gen [gen]
                  (-> gen
                      txn-generator
                      tag-rw
                      interleave-subscribes))))))
