(ns jepsen.redpanda.nemesis
  "Fault injection for Redpanda clusters"
  (:require [clojure [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [control :as c]
                    [db :as db]
                    [nemesis :as n]
                    [util :refer [pprint-str rand-nth-empty]]]
            [jepsen.control.net :as cn]
            [jepsen.nemesis [combined :as nc]
                            [membership :as membership]]
            [jepsen.redpanda [db :as rdb]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def min-cluster-size
  "How small do we allow clusters to become? I think 1 breaks Raft maybe?"
  3)

(def ip->node-cache
  "A map of IP addresses to node names. Lazily built on first use."
  (promise))

(defn ip->node
  "Turns an IP address into a node."
  [test ip]
  (when-not (realized? ip->node-cache)
    (deliver ip->node-cache
             (->> (c/on-nodes test (fn [test node] (cn/local-ip)))
                  (map (juxt second first))
                  (into {}))))
  (let [node (@ip->node-cache ip)]
    (assert+ node
             {:type ::no-node-with-ip
              :ip   ip
              :cache @ip->node-cache})
    node))

(defn node->ip
  "Turns a node into an IP address"
  [test node]
  (first (vals (c/on-nodes test [node]
                           (fn [test node]
                             (cn/ip node))))))

(defn find-by
  "Takes a function f, a value v, and a collection of xs. Returns the first x
  such that (= v (f x)). Helpful in extracting, say, a node with the given id
  from a list of node maps: (find-by :id 0 brokers)."
  [f v xs]
  (first (filter (fn [x] (= v (f x))) xs)))

(defn nodes-in-states
  "Takes a node state map from the membership and returns a vector of all nodes
  which are in the given set of states."
  [states nodes]
  (->> nodes
       (filter (comp states val))
       (mapv key)))

(defn known-to-active-majority?
  "Is a given node ID known to a majority of nodes in the :active state? Argh,
  this is such a race-condition-prone hack, but... hopefully it'll work
  sometimes?"
  [{:keys [node-views nodes]} id]
  (let [active (nodes-in-states #{:active} nodes)
        known (->> active
                   (keep (fn [node]
                           (->> (get node-views node)
                                (find-by :id id)))))]
    (< 1/2 (/ (count known) (count active)))))

(defn remove-node-op
  "We can remove a node from a cluster if it's in the view, and removing it
  wouldn't bring us below the minimum cluster size."
  [test {:keys [view nodes]}]
  (let [; What nodes, if we were to issue a remove command for them, would
        ; leave the cluster?
        active-set (set (nodes-in-states #{:active :adding} nodes))]
    ; What nodes *could* we try to remove? Note that it's important that we try
    ; to re-remove nodes which are already removing, because the request to
    ; remove that node might have crashed in an indeterminate way, resulting in
    ; a remove which would never actually complete unless we retry.
    (->> (nodes-in-states #{:adding :active :removing} nodes)
         (keep (fn [node]
                 (let [;ip (node->ip test node)
                       id (->> view (find-by :node node) :id)
                       ; What would the new active set be?
                       active' (disj active-set node)
                       ; Who should we ask to do the remove?
                       via (->> nodes
                                (nodes-in-states #{:active})
                                rand-nth-empty)]
                   (when (and id via (<= min-cluster-size (count active')))
                     {:type  :info
                      :f     :remove-node
                      :value {:node node, :id id, :via via}}))))
         vec
         rand-nth-empty)))

(defn free-node-op
  "Nodes which have been removed can be freed, killing the process, deleting
  their data files, and marking them for possible re-entry to the cluster."
  [test {:keys [nodes]}]
  (->> (nodes-in-states #{:removed} nodes)
       (map (fn [node]
              {:type  :info
               :f     :free-node
               :value node}))
       rand-nth-empty))

(defn add-node-op
  "We can add any free node to the cluster."
  [test {:keys [nodes]}]
  (->> (nodes-in-states #{:free} nodes)
       (map (fn [node]
              {:type  :info
               :f     :add-node
               :value {:node node}}))
       rand-nth-empty))

(defrecord Membership
  [node-views ; A map of nodes to that node's local view of the cluster
   view       ; Our merged view of cluster state
   pending    ; Pending [op op'] pairs of membership operations we're waiting
              ; to resolve
   nodes      ; A map of nodes to states. Node states are:
              ;  :active   In the cluster
              ;  :adding   Joining the cluster but not yet complete
              ;  :removing Being removed from the cluster but not yet complete
              ;  :removed  The cluster believes this node is gone, but we
              ;            haven't killed it and wiped its data files yet
              ;  :free     Not in the cluster at all; blank slate
   ]

  membership/State
  (setup! [this test]
    (assoc this
           :nodes (zipmap (:nodes test) (repeat :active))))

  (node-view [this test node]
    (->> (c/on-nodes test [node]
                     (fn fetch-node-view [test node]
                       (if-not (rdb/enabled?)
                         [] ; Not a part of the cluster
                         (when-let [info (rdb/cluster-info)]
                           (->> info
                                :brokers
                                (mapv (fn xform-broker [broker]
                                        {:node (ip->node test (:host broker))
                                         :id   (:id broker)})))))))
         first
         val))

  (merge-views [this test]
    ; Straightforward merge by node id, picking any representation of a node.
    (->> node-views
         (mapcat (fn [[observer view]]
                   view))
         (group-by :id)
         vals
         (mapv first)))

  (fs [this]
    #{:add-node
      :free-node
      :remove-node})

  (op [this test]
    (or (->> [(free-node-op   test this)
              (add-node-op    test this)
              (remove-node-op test this)]
             (remove nil?)
             rand-nth-empty)
        :pending))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :add-node
      (try+
        (let [this'  (update this :nodes assoc (:node value) :adding)
              node   (:node value)
              id     (rdb/gen-node-id! test node)]
          [(-> (c/on-nodes test [node]
                           (fn [test node]
                             (rdb/configure! test node)
                             (rdb/enable!)
                             (assoc op :value
                                    (assoc value
                                           :id id
                                           :result (db/start! (:db test)
                                                              test node)))))
               first
               val)
           this']))

      :free-node
      (let [node value]
        (c/on-nodes test [node] rdb/nuke!)
        [(assoc op :done? true)
         (update this :nodes assoc node :free)])

      :remove-node
      ; First, flag this node as being in state removing
      (let [this' (update this :nodes assoc (:node value) :removing)]
        ; Then try to actually remove it
        (try+
          (rdb/decommission! (:via value) (:id value))
          [(update op :value assoc :ok? true) this']
          (catch [:status 400] e
            ; When our request is rejected, we know the node isn't being
            ; removed and leave it in the original state.
            [(assoc op :error (:body e)) this])
          (catch [:status 500] e
            ; This could go either way, so we flag the node as :removing just
            ; in case.
            [(assoc op :error (:body e)) this'])
          (catch java.net.ConnectException e
           ; Can't have happened
           [(assoc op :error {:type :connect-exception}) this])))))

  (resolve [this test]
    (reduce
      (fn resolve-op [this [{:keys [f value] :as op} op' :as pair]]
        (cond ; It's possible to add and remove a node in rapid succession,
              ; such that the add op remains pending because we never got a
              ; chance to see it. Once a node is free, we clear all pending ops
              ; associated with that node, regardless of instance.
              (= :free (get (:nodes this) (:node (:value op))))
              (update this :pending disj pair)

              true
              (case f
                ; Once an adding node is known to an active majority, we
                ; consider it active, and mark its add op as resolved.
                :add-node
                (if-not (known-to-active-majority? this (:id (:value op')))
                  ; Still waiting
                  this
                  ; Accepted!
                  (let [node (:node value)]
                    (-> this
                        (update :nodes assoc node :active)
                        (update :pending disj pair))))

                ; Free always completes synchronously
                :free-node
                (update this :pending disj pair)

                :remove-node
                (cond ; Definitely didn't happen; we're done here.
                      (= 400 (:code (:error op')))
                      (update this :pending disj pair)

                      ; Likewise, can't have happened.
                      (= :connect-exception (:type (:error op')))
                      (update this :pending disj pair)

                      ; OK, this op might have or definitely did take place. But
                      ; if a majority of active nodes still think it's in the
                      ; cluster, we'll wait.
                      (known-to-active-majority? this (:id (:value op)))
                      this

                      ; Mostly forgotten--let's call this done!
                      :else
                      (let [node (:node (:value op))]
                        (-> this
                            (update :nodes assoc node :removed)
                            (update :pending disj pair))))

                ; Dunno how to resolve this :f
                nil
                this)))
      this
      pending))

  ; Unused; we implement resolve manually.
  (resolve-op [this test pair])

  (teardown! [this test]))

(defn membership-package
  "A nemesis package which can perform membership changes."
  [opts]
  (-> opts
      (assoc :membership {:state (map->Membership
                                   {})
                          :log-node-views? true
                          :log-resolve? true
                          :log-view? true})
      membership/package
      (assoc :perf #{{:name "member"
                      :fs   #{:add-node :free-node :remove-node}
                      :color "#A66AD8"}})))

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ...}"
  [opts]
  (-> opts
      nc/nemesis-packages
      (conj (membership-package opts))
      nc/compose-packages))
