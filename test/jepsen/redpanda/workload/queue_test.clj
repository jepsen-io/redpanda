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
             :skipped [:c]}
            {:key :x
             :ops [poll-4 poll-7]
             :skipped [:f]}]
           (-> [poll-1-2 poll-3 poll-4 write-6 poll-7]
               analysis
               :errors
               :poll-skip)))))
