(ns jepsen.redpanda.workload.queue-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]
                     [set :as set]]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]]
            [jepsen.redpanda.workload.queue :refer :all]))

(deftest log->last-index->values-test
  (testing "empty"
    (is (= [] (log->last-index->values []))))
  (testing "standard"
    (is (= [nil #{:a :b} nil #{:c} #{:d}]
           (log->last-index->values
             [nil #{:a} #{:a :b :c} nil #{:c} #{:c :d} #{:d}])))))

(deftest log->value->first-index-test
  (testing "empty"
    (is (= {} (log->value->first-index []))))
  (testing "standard"
    (is (= {:a 0, :b 1, :c 1, :d 3}
           (log->value->first-index
             [nil #{:a} #{:a :b :c} nil #{:c} #{:c :d} #{:d}])))))

(deftest version-orders-test
  ; Playing a little fast and loose here: when there's conflicts at an offset
  ; we choose a single value nondeterministically.
  (is (= {:orders {:x {:by-index   [:a :c :b :d]
                       :by-value   {:a 0, :b 2, :c 1, :d 3}
                       ; The raw log has a gap at offset 2.
                       :log        [#{:a} #{:b :c} nil #{:b} #{:d}]}}
          ; The write of c at 1 conflicts with the read of b at 1
          :errors [{:key :x, :index 1, :offset 1, :values #{:b :c}}]}

         (version-orders
           ; Read [a b] at offset 0 and 1
           [{:type :ok, :f :txn, :value [[:poll {:x [[0 :a] [1 :b]]}]]}
            ; But write c at offset 1, b at offset 3, and d at offset 4
            {:type :info, :f :txn, :value [[:send :x [1 :c]]
                                           [:send :x [3 :b]]
                                           [:send :x [4 :d]]]}])))

  (testing "a real-world example"
    (let [h [{:type :invoke, :f :send, :value [[:send 11 641]], :time 280153467070, :process 379}
{:type :ok, :f :send, :value [[:send 11 [537 641]]], :time 280169754615, :process 379}
{:type :invoke, :f :send, :value [[:send 11 645]], :time 283654729962, :process 363}
{:type :ok, :f :send, :value [[:send 11 [537 645]]], :time 287474569112, :process 363}
             ]]
      (is (= [{:key 11
               :index  0
               :offset 537
               :values #{641 645}}]
             (:errors (version-orders h)))))))



(deftest g1a-test
  ; If we can observe a failed write, we have a case of G1a.
  (let [send {:type :fail, :f :send, :value [[:send :x 2] [:send :y 3]]}
        poll {:type :ok,   :f :poll, :value [[:poll {:x [[0 2] [1 3]]}]]}]
    (is (= [{:op    poll
             :key   :x
             :value 2}]
           (-> [send poll] analysis :errors :g1a)))))

(deftest lost-update-test
  (testing "consistent"
    ; We submit a at offset 0, b at offset 1, and d at offset 3. A read observes
    ; c at offset 2, which implies we should also have read a and b.
    (let [send-a  {:type :ok, :f :send, :value [[:send :x [0 :a]]]}
          send-bd {:type :ok, :f :send, :value [[:send :x [1 :b]]
                                                [:send :x [3 :d]]]}
          poll {:type :ok, :f :poll, :value [[:poll {:x [[2 :c]]}]]}]
      (is (= [{:key :x
               :lost [:a :b]}]
             (-> [send-a send-bd poll] analysis :errors :lost-update)))))

  (testing "inconsistent"
    ; Here, we have inconsistent offsets. a is submitted at offset 0, but gets
    ; overwritten by b at offset 0. c appears at offset 2. We read c, which
    ; means we *also* should have read a and b; however, b's offset could win
    ; when we compute the version order. To compensate, we need more than the
    ; final version order indexes.
    (let [send-a {:type :ok, :f :send, :value [[:send :x [0 :a]]]}
          send-bc {:type :ok, :f :send, :value [[:send :x [0 :b]]
                                                [:send :x [2 :c]]]}
          read-bc {:type :ok, :f :poll, :value [[:poll {:x [[0 :b] [2 :c]]}]]}]
      (is (= [{:key :x
              :lost [:a]}]
             (-> [send-a send-bc read-bc] analysis :errors :lost-update))))))

(deftest poll-skip-test
  ; Process 0 observes offsets 1, 2, then 4, then 7, but we know 3 and 6
  ; existed due to other reads/writes. 5 might actually be a gap in the log.
  (let [poll-1-2 {:process 0, :f :poll, :value [[:poll {:x [[1 :a], [2 :b]]}]]}
        poll-4   {:process 0, :f :poll, :value [[:poll {:x [[4 :d]]}]]}
        poll-7   {:process 0, :f :poll, :value [[:poll {:x [[7 :g]]}]]}
        ; Reads and writes that let us know offsets 3 and 5 existed
        poll-3   {:process 1, :f :poll, :value [[:poll {:x [[3 :c]]}]]}
        write-6  {:process 2, :f :poll, :value [[:send :x [6 :f]]]}
        errs [{:key :x
               :ops [poll-1-2 poll-4]
               :delta 2
               :skipped [:c]}
              {:key :x
               :ops [poll-4 poll-7]
               :delta 2
               :skipped [:f]}]
        nm (comp :poll-skip :errors analysis)]
    (is (= errs (nm [poll-1-2 poll-3 poll-4 write-6 poll-7])))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow skips.
    (testing "with intermediate subscribe"
      (let [sub-xy     {:type :ok,   :process 0, :f :subscribe, :value [:x :y]}
            assign-xy  {:type :ok,   :process 0, :f :assign, :value [:x :y]}
            sub-y      {:type :ok,   :process 0, :f :subscribe, :value [:y]}
            assign-y   {:type :info, :process 0, :f :assign, :value [:y]}]
        (is (nil? (nm [poll-1-2 poll-3 sub-y    poll-4 assign-y write-6 poll-7])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [poll-1-2 poll-3 sub-xy poll-4
                         assign-xy write-6 poll-7])))))))

(deftest nonmonotonic-poll-test
  ; A nonmonotonic poll occurs when a single process performs two transactions,
  ; t1 and t2, both of which poll key k, and t2 begins with a value from k
  ; *prior* to t1's final value.
  ;
  ; Here process 0 polls 1 2 3, then goes back and reads 2 ... again.
  (let [poll-123 {:process 0, :f :poll, :value [[:poll {:x [[1 :a],
                                                            [2 :b]
                                                            [3 :c]]}]]}
        poll-234 {:process 0, :f :poll, :value [[:poll {:x [[2 :b]
                                                            [3 :c]
                                                            [4 :d]]}]]}
        nm (comp :nonmonotonic-poll :errors analysis)
        errs [{:key    :x
               :ops    [poll-123 poll-234]
               :values [:c :b]
               :delta  -1}]]
    (testing "together"
      (is (= errs (nm [poll-123 poll-234]))))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow nonmonotonicity.
    (testing "with intermediate subscribe"
      (let [sub-xy     {:type :ok,   :process 0, :f :subscribe, :value [:x :y]}
            assign-xy  {:type :ok,   :process 0, :f :assign, :value [:x :y]}
            sub-y      {:type :ok,   :process 0, :f :subscribe, :value [:y]}
            assign-y   {:type :info, :process 0, :f :assign, :value [:y]}]
        (is (nil? (nm [poll-123 sub-y poll-234])))
        (is (nil? (nm [poll-123 assign-y poll-234])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [poll-123 sub-xy poll-234])))
        (is (= errs (nm [poll-123 assign-xy poll-234])))))))

(deftest nonmonotonic-send-test
  ; A nonmonotonic send occurs when a single process performs two transactions
  ; t1 and t2, both of which send to key k, and t1's first send winds up
  ; ordered at or before t2's last send in the log.
  ;
  ; Here process 0 sends offsets 3, 4, then sends 1, 2
  (let [send-34 {:type :info, :process 0, :f :send, :value [[:send :x [3 :c]]
                                                            [:send :x [4 :d]]]}
        send-12 {:type :ok, :process 0, :f :send, :value [[:send :x [1 :a]]
                                                          [:send :x [2 :b]]]}
        errs [{:key    :x
               :values [:d :a]
               :delta  -3
               :ops    [send-34 send-12]}]
        nm (comp :nonmonotonic-send :errors analysis)]
    (is (= errs (nm [send-34 send-12])))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow nonmonotonicity.
    (testing "with intermediate subscribe"
      (let [sub-xy     {:type :ok,   :process 0, :f :subscribe, :value [:x :y]}
            assign-xy  {:type :ok,   :process 0, :f :assign, :value [:x :y]}
            sub-y      {:type :ok,   :process 0, :f :subscribe, :value [:y]}
            assign-y   {:type :info, :process 0, :f :assign, :value [:y]}]
        (is (nil? (nm [send-34 sub-y    send-12])))
        (is (nil? (nm [send-34 assign-y send-12])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [send-34 sub-xy    send-12])))
        (is (= errs (nm [send-34 assign-xy send-12])))))))

(deftest int-poll-skip-test
  ; An *internal poll skip* occurs when within the scope of a single
  ; transaction successive calls to poll() (or a single poll()) skip over a
  ; message we know exists.
  ;
  ; One op observes offsets 1 and 4, but another observes offset 2, which tells
  ; us a gap exists.
  (let [; Skip within a poll
        poll-1-4a {:f :poll, :value [[:poll {:x [[1 :a], [4 :d]]}]]}
        ; Skip between polls
        poll-1-4b {:f :poll, :value [[:poll {:x [[1 :a]]}]
                                     [:poll {:x [[4 :d]]}]]}
        poll-2 {:f :poll, :value [[:poll {:x [[2 :b]]}]]}]
    (is (= [{:key :x
             :values  [:a :d]
             :skipped [:b]
             :delta 2
             :op poll-1-4a}
            {:key :x
             :values  [:a :d]
             :skipped [:b]
             :delta 2
             :op poll-1-4b}]
           (-> [poll-1-4a poll-1-4b poll-2]
               analysis
               :errors
               :int-poll-skip)))))

(deftest int-send-skip-test
  ; An *internal send skip* occurs when within the scope of a single
  ; transaction successive calls to send() wind up inserting to offsets which
  ; have other offsets between them.
  ;
  ; Here a single op inserts mixed in with another. We know a's offset, but we
  ; don't know c's. A poll, however, tells us there exists a b between them,
  ; and that c's offset is 3.
  (let [send-13 {:type :ok, :f :send, :value [[:send :x [1 :a]] [:send :x :c]]}
        poll-23 {:type :ok, :f :poll, :value [[:poll {:x [[2 :b] [3 :c]]}]]}]
    (is (= [{:key     :x
             :values  [:a :c]
             :skipped [:b]
             :delta   2
             :op      send-13}]
           (-> [send-13 poll-23]
               analysis
               :errors
               :int-send-skip)))))

(deftest int-nonmonotonic-poll-test
  ; An *internal nonmonotonic poll* occurs within the scope of a single
  ; transaction, where one or more poll() calls yield a pair of values such
  ; that the former has an equal or higher offset than the latter.
  (let [poll-31a {:f :poll, :value [[:poll {:x [[3 :c] [1 :a]]}]]}
        ; This read of :b tells us there was an index between :a and :c; the
        ; delta is therefore -2.
        poll-33b {:f :poll, :value [[:poll {:x [[2 :b] [3 :c]]}]
                                    [:poll {:x [[3 :c]]}]]}]
    (is (= [{:key    :x
             :values [:c :a]
             :delta  -2
             :op poll-31a}
            {:key    :x
             :values [:c :c]
             :delta  0
             :op     poll-33b}]
           (-> [poll-31a poll-33b] analysis :errors :int-nonmonotonic-poll)))))

(deftest int-nonmonotonic-send-test
  ; An *internal nonmonotonic send* occurs within the scope of a single
  ; transaction, where two calls to send() insert values in an order which
  ; contradicts the version order.
  (let [; In this case, the offsets are directly out of order.
        send-31a {:type :ok, :f :send, :value [[:send :x [3 :c]]
                                               [:send :x [1 :a]]]}
        ; Or we can infer the order contradiction from poll offsets
        send-42b {:type :info, :f :send, :value [[:send :y :d] [:send :y :b]]}
        poll-42b {:type :info, :f :poll, :value [[:poll {:y [[2 :b]
                                                             [3 :c]
                                                             [4 :d]]}]]}]
    (is (= [{:key    :x
             :values [:c :a]
             :delta  -1
             :op     send-31a}
            {:key    :y
             :values [:d :b]
             :delta  -2
             :op     send-42b}]
           (-> [send-31a send-42b poll-42b]
               analysis :errors :int-nonmonotonic-send)))))

(deftest duplicate-test
  ; A duplicate here means that a single value winds up at multiple positions
  ; in the log--reading the same log offset multiple times is a nonmonotonic
  ; poll.
  (let [; Here we have a send operation which puts a to 1, and a poll which
        ; reads a at 3; it must have been at both.
        send-a1 {:type :ok, :f :send, :value [[:send :x [1 :a]]]}
        poll-a3 {:type :ok, :f :poll, :value [[:poll {:x [[2 :b] [3 :a]]}]]}]
    (is (= [{:key   :x
             :value :a
             :count 2}]
           (-> [send-a1 poll-a3] analysis :errors :duplicate)))))
