(ns jepsen.redpanda.nemesis
  "Fault injection for Redpanda clusters"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.nemesis [combined :as nc]]))

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ...}"
  [opts]
  (-> opts
      nc/nemesis-packages
      nc/compose-packages))
