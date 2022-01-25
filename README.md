# jepsen.redpanda

Tests for the Redpanda distributed queue.

## Installation

You'll need a Jepsen environment: see
[https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment](Jepsen's
setup documentation) for details. In short, this means a cluster of 3+ Debian
Buster *DB nodes* to run instances of Redpanda, and a *control node* to run
this test harness. The control node needs a JDK, JNA, Leiningen, Gnuplot, and
Graphviz. On the control node, plan on ~24G of memory and about 100 GB of disk, if you want to run a few hundred ~1000 second tests back to back.

Once you have a Jepsen cluster set up, you'll need a copy of this test harness.
That can be a tarball or git clone of this repository, or you can compile this
test to a fat jar using `lein uberjar`, copy that jar to your control node,
and invoke it using `java -jar <jar-file> <test args ...>`. In these docs,
we'll assume you've got a copy of this directory and are invoking tests via
`lein run`.

## Quickstart

For these examples, we'll assume you have a file `~/nodes` which has your DB
node hostnames, one per line, and that your user with sudo access on each node
is named `admin. This is the setup you'd get out of the box from the [Jepsen
Cloudformation deployment on the AWS
Marketplace](https://aws.amazon.com/marketplace/pp/prodview-ykhheuyq5qdnq). All
these commands should be run in the top-level directory of this repository.

To run a very brief test, just to make sure everything works:

```
lein run test --nodes-file ~/nodes --username admin
```

This will likely print something like:

```clj
 ...
 :valid? true}


Everything looks good! ヽ(‘ー`)ノ
```

To observe inconsistent offsets and/or duplicates in Redpanda 21.10.1, try:

```
lein run test --nodes-file ~/nodes --username admin -s --nemesis pause,kill --time-limit 300 --test-count 5
```

`-s` asks for safer options than the defaults: it turns on idempotence, sets
auto-offset-reset=earliest, etc. `--nemesis pause,kill` asks the nemesis to
pause and kill random nodes throughout the test. We also increase the time of
each test to 300 seconds, and run up to 5 tests in a row, stopping as soon as
the test detects an error. This might emit something like:

```clj
 :workload {:valid? false
            :duplicate {:count 1,
                        :errs ({:key 8, :value 711, :count 2})},
```

If we run the test against version 21.11.2, we should no longer see duplicates
or inconsistent offsets. Instead, we might observe lost/stale messages,
reported as `unseen`:

```
lein run test --nodes-file ~/nodes --username admin --nemesis pause,kill --time-limit 300 --test-count 5 --version 21.11.2-1-f58e69b6
```

To start a web server which lets you browse the results of these tests on port
8080, run:

```
lein run serve
```

## Usage

`lein run test` runs a single test--possibly multiple times in a row.

`lein run test-all` runs a whole slew of different tests, including multiple
workloads, versions, combinations of nemeses, with and without transactions.
You can constrain any of these--for example, to compare behavior under network
partitions across two different versions, both with and without transactions:

```clj
lein run test-all --nodes-file ~/nodes --username admin --nemesis partition --time-limit 300 --versions 21.10.1-1-e7b6714a,21.11.2-1-f58e69b6
```

`lein run serve` runs a web server to browse results of tests from the local
`store/` directory. There's nothing magic about this directory--you can tar it
up and copy it around, and it'll still work fine. You can copy one store
directory's contents into another and that'll work too, which might be helpful
for CI or farming out tests to lots of clusters.

All three of these commands provide docs for all of their options: try `lein
run test -h` for help on running a single test.

## What's Here

### Overall Structure

`project.clj` defines this test's version, dependencies, JVM options, and entry
point; it's read by Leiningen. Source code for the test suite lives in `src/`.
Tests for that testing code live in `test/`. In both of these directories,
folder structure maps to namespaces: the file `src/jepsen/redpanda/core.clj`
defines the `jepsen.redpanda.core` namespace. The `store/` directory stores the
results of any tests you might run.

The top-level namespace for this test is `jepsen.redpanda.core`, which defines
CLI options and constructs tests to run, then passes them to Jepsen for
execution. `jepsen.redpanda.db.redpanda` and `jepsen.redpanda.db.kafka` defines
database setup and teardown for Redpanda and Kafka, respectively.
`jepsen.redpanda.client` is for working with Kafka clients.
`jepsen.redpanda.nemesis` handles fault injection: most notably, the cluster
membership state machine. Workloads live in `jepsen.redpanda.workload.queue`
and jepsen.redpanda.workload.list-append`.

### Workloads

This test comprises two workloads.

The main workload, `queue`, performs both transactional and non-transactional
sends and polls, mixed with calls to `assign` or `subscribe`. At the end of the
test, it tries to read everything that was written via a series of final polls.
It has a sophisticated family of analyzers which look for duplicate writes,
inconsistent offsets, places where consumers or producers jump forward or
backwards in offsets, aborted reads, and some basic cycles like G0 and G1c.

The second workload, `list-append`, is much simpler: it performs sends much
like `queue`, but for any read, attempts to read the *entire* topic-partition
based on the most recent offset.

Both workloads spread their operations across a rotating pool of
topic-partitions, creating new topics once existing topics reach a certain
threshold of writes. You can choose which workload is run via `-w queue` or `-w
list-append`.

### Tests for Tests

This test harness also comes with its own tests--mainly for the queue
workload's various analyzers. These tests live in `test/`, and can be run via
`lein test`---not to be confused with `lein run test`, which runs the Jepsen
test itself. Use these if you wind up changing the checkers somehow, to make
sure they still detect the anomalies they ought to. These tests can also be
helpful in understanding why the various analyzer functions work the way they
do, and what they consider an anomaly.

## Using the REPL

Sometimes you need to explore a test's history in more detail. `lein repl` will
spawn a Clojure repl with all the test suite's code available. To start with,
you might want a few namespaces and functions available:

```clj
jepsen.redpanda.core=> (require '[jepsen.store :as s] '[jepsen.checker :refer [check]] '[jepsen.redpanda.workload.queue :as q] '[jepsen.redpanda.client :as c])
```

We can load a test from disk using `jepsen.store/test`. It can take a path to a
particular test's directory in `store/`. As a shortcut, clicking the title of a
test directory in the web interface will copy this path as a string, so you can
paste it right into the REPL.

```clj
jepsen.redpanda.core=> (def t (s/test "/home/aphyr/redpanda/store/2022-01-19.deb queue subscribe acks=all retries=1000 aor=earliest default-r=3 auto-topics=false idem=true pause/20220120T220302.000-0500"))
```

Was this test valid?

```clj
jepsen.redpanda.core=> (:valid? (:results t))
false
```

No! Why not?

```clj
jepsen.redpanda.core=> (->> t :results :workload pprint)
... eight billion lines ...
     :time 63860531157,
     :process 131,
     :index 7195}}]}}
```

Well, that's a lot to read. What keys are in this map?

```clj
jepsen.redpanda.core=> (->> t :results :workload keys)
(:valid? :unseen :poll-skip :info-txn-causes :worst-realtime-lag :int-send-skip :lost-write :nonmonotonic-poll :error-types :int-poll-skip :int-nonmonotonic-poll)
```

Right, let's look at the error types:

```clj
jepsen.redpanda.core=> (->> t :results :workload :error-types)
[:int-nonmonotonic-poll :int-poll-skip :int-send-skip :lost-write :nonmonotonic-poll :poll-skip :unseen]
```

All *kinds* of cool stuff here. How many lost-write errors?

```clj
jepsen.redpanda.core=> (->> t :results :workload :lost-write :count)
4
```

What was the first?

```clj
jepsen.redpanda.core=> (->> t :results :workload :lost-write :errs first pprint)
{:key 54,
 :value 436,
 :index 401,
 :max-read-index 977,
 :writer
 {:type :ok,
  :f :txn,
  :value [[:send 54 [1114 436]] [:poll {}] [:poll {}]],
  :time 800986095314,
  :process 648,
  :rebalance-log
  [{:type :revoked, :keys [55 4]} {:type :revoked, :keys [53]}],
  :index 91528},
 :max-read
 {:type :ok,
  :f :txn,
  :value
  [[:poll ...]
   [:poll ...]
   [:send 56 [1725 661]]
   [:send 55 [2245 800]]],
  :time 828187300771,
  :process 590,
  :index 95996}}
```

OK, so *something* went wrong on key 54, around value 436. What operation wrote that? The result claims it has the writer here, but just to double-check, let's look at the history ourselves.

```clj
jepsen.redpanda.core=> (->> t :history (q/writes-of-key-value 54 436) pprint)
({:type :invoke,
  :f :txn,
  :value [[:send 54 436] [:poll] [:poll]],
  :time 791552742537,
  :process 648}
 {:type :ok,
  :f :txn,
  :value [[:send 54 [1114 436]] [:poll {}] [:poll {}]],
  :time 800986095314,
  :process 648,
  :rebalance-log
  [{:type :revoked, :keys [55 4]} {:type :revoked, :keys [53]}]})
```

Here's the invocation and the completion of the operation which wrote key 54.
Sure enough, it appears to have suceeded! Was it ever read?

```clj
jepsen.redpanda.core=> (->> t :history (q/reads-of-key-value 54 436) pprint)
()
```

Nothing ever read it. What about *nearby* messages? We know this write
ostensibly went to offset 1114--let's look at everything that interacted with
the local neighborhood of, say, five offsets before and after 1114.

```clj
jepsen.redpanda.core=> (->> t :history (q/around-key-offset 54 1114 5) pprint)
({:type :ok,
  :f :txn,
  :value ([:send 54 [1117 427]]),
  :time 791596972858,
  :process 778}
 {:type :ok,
  :f :txn,
  :value ([:send 54 [1110 434]]),
  :time 791613396199,
  :process 584}
 {:type :ok,
  :f :txn,
  :value ([:send 54 [1114 436]]),
  :time 800986095314,
  :process 648,
  :rebalance-log
  [{:type :revoked, :keys [55 4]} {:type :revoked, :keys [53]}]}
 {:type :ok,
  :f :txn,
  :value ([:poll {54 ([1110 434] [1117 427])}]),
  :time 828187300771,
  :process 590}
 ... lots more polls ...
 {:f :poll,
  :value ([:poll {54 ([1110 434] [1117 427])}]),
  :poll-ms 1000,
  :time 1039009647614,
  :process 868,
  :type :ok})
```

So we successfully wrote 427 to offset 1117, 434 to offset 1110, and 436 to
offset 1114. Yet somehow when we went to read anything in this neighborhood, we
only observed values 434 and 427---no 436! This very much looks to be a lost
write! Note that around-key-offset has *trimmed* the polls and sends inside
each operation in order to show us only those parts relevant to this particular
key and region of offsets. The real poll operations here have hundreds of
messages each, so this is much easier to read.

You'll find several more functions for slicing and dicing history operations in
`jepsen.redpanda.workload.queue`.

## FAQ

If you're running in containers Redpanda may fail to start, citing a need to
increase `/proc/sys/fs/aio-max-nr` on the host OS--individual containers can't
alter it themselves. Try

```
sudo sh -c 'echo 10000000 > /proc/sys/fs/aio-max-nr'
```

## License

Copyright © 2021, 2022 Jepsen. LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
