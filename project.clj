(defproject jepsen.redpanda "0.1.0-SNAPSHOT"
  :description "Tests for the RedPanda distributed queuing system"
  :url "https://github.com/jepsen-io/redpanda"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clj-http "3.12.3"]
                 [cheshire "5.10.1"]
                 [jepsen "0.2.6-SNAPSHOT"]
                 [org.apache.kafka/kafka-clients "3.0.0"]]
  :main jepsen.redpanda.core
  :repl-options {:init-ns jepsen.redpanda.core})
