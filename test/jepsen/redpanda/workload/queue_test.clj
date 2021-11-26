(ns jepsen.redpanda.workload.queue-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]
                     [set :as set]]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]]
            [jepsen.redpanda.workload.queue :refer :all]))

(deftest version-orders-test
  ; Playing a little fast and loose here: when there's conflicts at an offset
  ; we choose a single value nondeterministically.
  (is (= {:orders {:x {:by-index [:a :c :d]
                       :by-value {:a 0, :c 1, :d 2}}}
          ; The write of c at 1 conflicts with the read of b at 1
          :errors [{:key :x, :offset 1, :values #{:b :c}}]}
         (version-orders
           ; Read [a b]
           [{:type :ok, :f :txn, :value [[:poll {:x [[0 :a] [1 :b]]}]]}
            ; But write c at offset 1, and d at offset 4
            {:type :info, :f :txn, :value [[:send :x [1 :c]]
                                           [:send :x [4 :d]]]}]))))

(deftest g1a-test
  ; If we can observe a failed write, we have a case of G1a.
  (let [send {:type :fail, :f :send, :value [[:send :x 2] [:send :y 3]]}
        poll {:type :ok,   :f :poll, :value [[:poll {:x [[0 2] [1 3]]}]]}]
    (is (= [{:op    poll
             :key   :x
             :value 2}]
           (-> [send poll] analysis :errors :g1a)))))

(deftest lost-update-test
  ; We submit a at offset 0, b at offset 1, and d at offset 3. A read observes
  ; c at offset 2, which implies we should also have read a and b.
  (let [send-a  {:type :ok, :f :send, :value [[:send :x [0 :a]]]}
        send-bd {:type :ok, :f :send, :value [[:send :x [1 :b]]
                                              [:send :x [3 :d]]]}
        poll {:type :ok, :f :poll, :value [[:poll {:x [[2 :c]]}]]}]
    (is (= [{:key :x
             :lost [:a :b]}]
           (-> [send-a send-bd poll] analysis :errors :lost-update)))))

(deftest poll-skip-test
  ; Process 0 observes offsets 1, 2, then 4, then 7, but we know 3 and 6
  ; existed due to other reads/writes. 5 might actually be a gap in the log.
  (let [poll-1-2 {:process 0, :f :poll, :value [[:poll {:x [[1 :a], [2 :b]]}]]}
        poll-4   {:process 0, :f :poll, :value [[:poll {:x [[4 :d]]}]]}
        poll-7   {:process 0, :f :poll, :value [[:poll {:x [[7 :g]]}]]}
        ; Reads and writes that let us know offsets 3 and 5 existed
        poll-3   {:process 1, :f :poll, :value [[:poll {:x [[3 :c]]}]]}
        write-6  {:process 2, :f :poll, :value [[:send :x [6 :f]]]}]
    (is (= [{:key :x
             :ops [poll-1-2 poll-4]
             :delta 2
             :skipped [:c]}
            {:key :x
             :ops [poll-4 poll-7]
             :delta 2
             :skipped [:f]}]
           (-> [poll-1-2 poll-3 poll-4 write-6 poll-7]
               analysis
               :errors
               :poll-skip)))))

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
                                                            [4 :d]]}]]}]
    (is (= [{:key    :x
             :ops    [poll-123 poll-234]
             :values [:c :b]
             :delta  -2}]
           (-> [poll-123 poll-234]
               analysis
               :errors
               :nonmonotonic-poll)))))

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
