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
  "How small do we allow clusters to become?"
  1)

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

(defn remove-node-op
  "We can remove a node from a cluster if it's in the view, and removing it
  wouldn't bring us below the minimum cluster size."
  [test {:keys [view nodes]}]
  (let [; What nodes, if we were to issue a remove command for them, would
        ; leave the cluster?
        active-set (set (nodes-in-states #{:active :adding} nodes))]
    ; What nodes *could* we try to remove?
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

(defrecord Membership
  [node-views ; A map of nodes to that node's local view of the cluster
   view       ; Our merged view of cluster state
   pending    ; Pending [op op'] pairs of membership operations we're waiting
              ; to resolve
   nodes      ; A map of nodes to states. Node states are:
              ;  :active   In the cluster
              ;  :adding   Joining the cluster but not yet complete
              ;  :removing Being removed from the cluster but not yet complete
              ;  :free     Not in the cluster at all
   ]

  membership/State
  (setup! [this test]
    (assoc this
           :nodes (zipmap (:nodes test) (repeat :active))))

  (node-view [this test node]
    (->> (c/on-nodes test [node]
                     (fn fetch-node-view [test node]
                       (->> (rdb/cluster-info)
                            :brokers
                            (mapv (fn xform-broker [broker]
                                    {:node (ip->node test (:host broker))
                                     :id   (:id broker)})))))
         first
         val))

  (merge-views [this test]
    ; Straightforward merge by node, picking any representation of a node.
    (->> node-views
         (mapcat (fn [[observer view]]
                   view))
         (group-by :node)
         vals
         (mapv first)))

  (fs [this]
    #{:add-node :remove-node})

  (op [this test]
    (or (->> [(remove-node-op test this)]
             (remove nil?)
             rand-nth-empty)
        :pending))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      op))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    this)

  (teardown! [this test]
    ))

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
                      :fs   #{:add-node :remove-node :stake}
                      :color "#A66AD8"}})))

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ...}"
  [opts]
  (-> opts
      nc/nemesis-packages
      (conj (membership-package opts))
      nc/compose-packages))
