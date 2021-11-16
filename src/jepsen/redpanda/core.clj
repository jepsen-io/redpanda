(ns jepsen.redpanda.core
  "Entry point for command line runner. Constructs tests and runs them."
  (:require [jepsen [cli :as cli]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.redpanda [db :as db]]))

(defn redpanda-test
  "Constructs a test for RedPanda from parsed CLI options."
  [opts]
  (let []
    (merge tests/noop-test
           opts
           {:name "redpanda"
            :db   (db/db)
            :os   debian/os})))

(def cli-opts
  "Command line options."
  [])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn redpanda-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
