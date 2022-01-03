(ns jepsen.redpanda.nemesis
  "Fault injection for Redpanda clusters"
  (:require [clojure [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [control :as c]
                    [db :as db]
                    [nemesis :as n]
                    [util :as util :refer [pprint-str rand-nth-empty max-by]]]
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
                                :brokers
                                (find-by :id id)))))]
    (if (< (count active) 3)
      (do (warn "Fewer than 3 active nodes in cluster:" (pr-str active) "\n"
                (pprint-str nodes)
                "\n\n"
                (pprint-str node-views))
          false)
      (< 1/2 (/ (count known) (count active))))))

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
                       id (->> view :brokers (find-by :node node) :id)
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
               :value {:node node}}))
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

(defn resolve-op
  "Takes a membership state and an [op op'] pair of nemesis operations within
  its pending set. Attempts to resolve that operation, returning a new
  membership state."
  [this [{:keys [f value] :as op} op' :as pair]]
  (cond ; It's possible to add and remove a node in rapid succession,
        ; such that the add op remains pending because we never got a chance to
        ; see it. Once a node is free, we clear all pending ops associated with
        ; that node, regardless of instance.
        (= :free (get (:nodes this) (:node (:value op))))
        (update this :pending disj pair)

        true
        (case f
          ; Once an adding node is known to be active in the current view, we
          ; consider it active, and mark its add op as resolved.
          :add-node
          (if (->> this :view :brokers
                   (find-by :id (:id (:value op')))
                   :status (= :active))
            ; Accepted!
            (let [node (:node value)]
              (-> this
                  (update :nodes assoc node :active)
                  (update :pending disj pair)))
            this) ; Still waiting!

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

                ; OK, this op might have or definitely did take place. But if a
                ; majority of active nodes still think it's in the cluster,
                ; we'll wait.
                :else
                (let [state (->> this :view :brokers
                                 (find-by :id (:id (:value op))))]
                  (info "Removing node" (:value op) "has state" (pr-str state))
                  (if state
                    ; Node still in cluster; not done!
                    this
                    ; Done!
                    (-> this
                        (update :nodes assoc (:node (:value op)) :removed)
                        (update :pending disj pair)))))

          ; Dunno how to resolve this :f
          nil
          this)))

(defrecord Membership
  [node-views ; A map of nodes to that node's local view of the cluster
   view       ; Our merged view of cluster state. Looks like:
              ; {:version 123
              ;  :brokers [{:node "n1", :id 3, :alive? true, :status :active}]}
   pending    ; Pending [op op'] pairs of membership operations we're waiting
              ; to resolve
   nodes      ; A map of nodes to states. Node states are:
              ;  :init     Freshly joined. We need to wait for these to be
              ;            reflected on all test nodes before we
              ;            consider them :active.
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
           :nodes (zipmap (:nodes test) (repeat :init))))

  (node-view [this test node]
    (try+ (let [enabled? (-> test
                             (c/on-nodes [node]
                                         (fn enabled? [_ _] (rdb/enabled?)))
                             (get node))]
            (assert (or (= true enabled?) (= false enabled?))
                    (str "Expected bool, got " (pr-str enabled?)))
            (if-not enabled?
              {:version ##-Inf} ; Not in cluster
              (let [{:keys [version brokers]} (rdb/cluster-view node)]
                {:version version
                 :brokers
                 (map (fn broker [{:keys [node_id membership_status is_alive]}]
                        {:id node_id
                         :node (rdb/node-id->node test node_id)
                         :status (keyword membership_status)
                         :alive? is_alive})
                      (sort-by :node_id brokers))})))
          (catch java.net.ConnectException e
            nil)
          (catch [:status 503] e
            (warn "Node" node
                  "returned 503 for membership view:" (pr-str e))
            nil)
          (catch [:status 404] _
            (warn "Node" node
                  "returned 404 for membership view--does it support this API?")
            nil)))

  (merge-views [this test]
    ; Pick the highest version
    (->> node-views vals (max-by :version)))

  (fs [this]
    #{:add-node
      :free-node
      :remove-node})

  (op [this test]
    (if-let [op (->> [(free-node-op   test this)
                      (add-node-op    test this)
                      (remove-node-op test this)]
                     (remove nil?)
                     rand-nth-empty)]
      (assoc-in op [:value :view-version] (:version view))
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
      (let [node (:node value)]
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
          (catch java.net.SocketTimeoutException e
            ; Not sure if this happened or not
            [(assoc op :error {:type :socket-timeout}) this'])
          (catch java.net.ConnectException e
           ; Can't have happened
           [(assoc op :error {:type :connect-exception}) this])))))

  (resolve [this test]
    ; When we start off, we've got nodes in state :init--we try to transition
    ; those to :active as soon as they're present in the view. This is our
    ; bootstrapping process--we can't trust the active set on startup because
    ; cluster join might be incomplete, leaving some "active" nodes not
    ; actually knowing who's in the cluster.
    (let [this (reduce (fn check-init-join [this node]
                         (if (->> this :view :brokers (find-by :node node))
                           ; This node is now in the cluster view.
                           (update this :nodes assoc node :active)
                           ; Still waiting
                           this))
                       this
                       (nodes-in-states #{:init} (:nodes this)))]
      ; Now handle pending ops
      (reduce resolve-op this pending)))

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
