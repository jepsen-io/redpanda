(defproject io.jepsen/redpanda "0.1.2-SNAPSHOT"
  :description "Tests for the RedPanda distributed queuing system"
  :url "https://github.com/jepsen-io/redpanda"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [clj-http "3.13.0"
                  :exclusions [riddley]]
                 [cheshire "5.13.0"]
                 [jepsen "0.3.6-SNAPSHOT"]
                 [org.apache.kafka/kafka-clients "3.8.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 ]
  :main jepsen.redpanda.core
  :test-selectors {:focus :focus}
  :repl-options {:init-ns jepsen.redpanda.core}
  :jvm-opts ["-server"
             ;"-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             ; GC tuning--see
             ; https://wiki.openjdk.java.net/display/shenandoah/Main
             ; https://wiki.openjdk.java.net/display/zgc/Main
             ;"-XX+UseZGC"
             ;"-XX+UseShenandoahGC"
             "-Xmx24g"
             ;"-XX:+UseLargePages" ; requires users do so some OS-level config
             "-XX:+AlwaysPreTouch"
             ; Instrumentation
             ;"-agentpath:/home/aphyr/yourkit/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,exceptions=disable,delay=10000"
             ])
