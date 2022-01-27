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

Or we could run the test against version 21.11.2, with process crashes and membership changes, and for significantly longer. This should, after a few hours, demonstrate lost/stale messages, reported as `unseen`:

```
lein run test --nodes-file ~/nodes --username admin -s --no-ww-deps --nemesis kill,membership --time-limit 1000 --test-count 10 --version 21.11.2-1-f58e69b6
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

## Understanding Results

In each test directory in `store/` you'll find several files. For starters, there's one directory for every DB node, which contains:

- `data.tar.bz2`: a tarball of that node's Redpanda data directory
- `redpanda.log`: the stdout/stderr logs of the Redpanda server
- `tcpdump.pcap`: If running with `--tcpdump`, a packet capture file of this node's kafka-protocol network traffic.

Three files show the chronology of the test. `jepsen.log` has the full output
of the test run--everything printed to the console. `history.edn` is a
machine-readable file with all the logical operations Jepsen performed during
the test. `history.txt` is the same, but as a tab-aligned text file.

`latency-raw.png` and `latency-quantiles.png` show the latency of each
operation over time. Colors denote successful (ok), failed (fail), or unknown
(info) operations. Shape denotes the logical function `:f` of that
operation--for example, a `poll`, `subscribe`, `txn`. Nemesis activity is shown as colored bars along the top of the plot, and vertical lines whenever the nemesis changes something.

`rate.png` shows the rate of requests per second, over time, with the same shapes and colors as the latency plot.

`unseen.png` shows the number of messages which were committed but not read by
any poller over time, broken out by key (partition-topic). Two final vertical
lines show the start and stop of the final polling period, when the cluster has
healed and we're trying to read all writes. This graph should go to zero
relatively quickly during the recovery window: if it doesn't, that suggests
some writes have been lost or are severely delayed.

`realtime-lag` plots shows how far behind the most recent committed write each
consumer is, over time. These come in several families, aggregated by key, by
worker thread (which maps to one client at any given time), or by neither.
Whenever we subscribe or assign a fresh topic, this might jump up, but it
should head down as consumers catch up to the most recent values. You can use
these plots to identify whether a single key or single thread has gotten stuck,
and whether other threads or keys are making progress.

`test.fressian` and `test.jepsen` have binary representations of the test in
its entirety.

`elle/` shows cycles between transactions, both as text files and as svg graphs. These are categorized by anomaly type--e.g. G0, G1c, etc.

`orders` appears whenever we see Weird Things Happen on some key. It contains
one SVG file for each key that did something odd, and shows the view of each
`ok` operation into that particular key's offsets. Time flows top to bottom,
and offsets are arranged left to right. Each number is the value of the message
at that particular offset. Hovering over any row shows more information. This
is helpful for understanding when there are reorderings or message loss.

`results.edn` has the results of the checker.

### Results In Depth

`results.edn` is a map with several keys. At each layer, the `:valid?` key says
whether the results (or part thereof) were considered valid, or if an anomaly was detected.

The `stats` part of the analysis provides overall statistics on the number of
operations, how many were successful (ok), failed (fail), or indeterminate
(info). These are also broken down by function, so you can see the behavior of
(e.g.) just `poll` operations.

```clj
{:stats {:valid? true,
         :count 14772,
         :ok-count 14653,
         :fail-count 0,
         :info-count 119,
         :by-f {:assign {:valid? true,
                         :count 1901,
                         :ok-count 1901,
                         :fail-count 0,
                         :info-count 0},
                :crash {:valid? false,
                        :count 8,
                        :ok-count 0,
                        :fail-count 0,
                        :info-count 8},
                :debug-topic-partitions {:valid? true,
                                         :count 8,
                                         :ok-count 8,
                                         :fail-count 0,
                                         :info-count 0},
                :poll {:valid? true,
                       :count 6551,
                       :ok-count 6551,
                       :fail-count 0,
                       :info-count 0},
                :send {:valid? true,
                       :count 6304,
                       :ok-count 6193,
                       :fail-count 0,
                       :info-count 111}}},
```

`clock` and `perf` are trivially true; they generate the clock, latency, and
rate plots as a side effect.

```clj
 :clock {:valid? true},
 :perf {:latency-graph {:valid? true},
        :rate-graph {:valid? true},
        :valid? true},
```

`ex` tracks exceptions thrown by Jepsen Clients during the test. When
exceptions appear here, you may want to add them to the error handling in that
particular client. It is, in general, always safe to allow them to
throw---explicitly handling these errors improves test specificity and
performance.

`assert` looks for assertion errors thrown by the Redpanda server, by parsing
logfiles.

`workload` contains workload-specific results.

#### Queue results

The queue workload includes two important keys: `:error-types`, which shows all
"interesting" behaviors observed during the test, and `:bad-error-types`, which
are those we think are specifically illegal given the test being run. For
example:

            :error-types (:G0
                          :duplicate
                          :int-nonmonotonic-poll
                          :int-poll-skip
                          :poll-skip),
            :bad-error-types (:duplicate
                              :int-nonmonotonic-poll
                              :int-poll-skip
                              :poll-skip)},

Here we detected a G0 anomaly, but because those happen *normally* in the Kafka
transaction model, we didn't flag it as a "bad" error. The duplicate, internal
nonmonotonic polls and poll skips, and external poll skips, caused this test to
fail.

Each error type has a corresponding key in `:errors` part of the workload
results. Examples of each type of error follow. For simplicity, these examples
are drawn from the test suite's internal tests; they use keys like `:x` rather
than integers, but should otherwise be structurally alike to those reported by
the real test harness.

`:inconsistent-offsets` signifies that a single offset in some key's log
contained multiple values. The offset-value mapping, called a `version order`,
is derived from both send and poll operations. This example tells us that key
`:x`, at offset 0, contained both values `1` and `2`. `:index` is a dense
offset, without gaps.

```clj
{:inconsistent-offsets
 ({:key :x, :offset 0, :index 0, :values #{1 2}})}
```

`:G1a` reports cases where an operation definitely failed, but one of its
writes appeared in a poll operation. For instance, this error shows that on key
`:x`, value `2` was written by a send operation which failed, but that send was
later observed by `:reader`.

```clj
{:G1a
 ({:key :x,
   :value 2,
   :writer
   {:index 1,
    :time 1,
    :process 0,
    :type :fail,
    :f :send,
    :value [[:send :x 2] [:send :y 3]]},
   :reader
   {:index 3,
    :time 3,
    :process 1,
    :type :ok,
    :f :poll,
    :value [[:poll {:x [[0 2]]}]]}})}
```

`:lost-write` finds cases where a known-successful send occurs at offset a, and
some offset b (such that a < b) is polled, and the message at offset a never
appears to any poll. This example shows that on key `:x`, value `:a` was lost.
It was written at index 0 in the log for `:x`, and the highest index which was
observed in some poll was 2. The writer of value `:a` is given, and so is the
reader which polled index 2. Because we expect pollers (across all consumers,
at least) observe values without gaps, this read of offset 2 implies we should
also have read offset 0--and yet no such read was found in this history.

```clj
{:lost-write
 ({:key :x,
   :value :a,
   :index 0,
   :max-read-index 2,
   :writer
   {:index 1,
    :time 1,
    :process 0,
    :type :ok,
    :f :send,
    :value [[:send :x [0 :a]]]},
   :max-read
   {:index 7,
    :time 7,
    :process 0,
    :type :ok,
    :f :poll,
    :value [[:poll {:x [[2 :c]]}]]}}
```

Note that this analysis is sophisticated enough to reason about inconsistent
offsets conservatively. Not all parts of the checker do this, but lost-writes
is careful to keep track of multiple indexes for a given message value, and
multiple values at a given index.

The lost-write checker also helps verify transactional atomicity: reading one
part of a transaction lets the checker prove that all the other writes must
have been written also---even if the writing transaction was itself
indeterminate.

`:poll-skip` finds places where a poller unexpectedly jumps over some offsets in
the log during two successive calls to `poll` performed by *different*
operations on the same client, and there was no call to `assign` or `subscribe`
between those polls which would have caused the consumer to forget the offset
it was tracking. This example shows that on key `:x`, a single consumer jumped
2 indexes forward in the log, skipping over value `:c`. The two operations are
shown: the first polled `:a` and `:b` at indexes 1 and 2, and the second polled
`:d` at index 4. The checker here knew that there existed a value `:c` at index
3 between these two polls, which went unobserved. `:delta` is the number of
indexes between the two polls--if pollers read in perfect order, we'd expect
this to always be 1.

```clj
{:poll-skip
 ({:key :x,
   :delta 2,
   :skipped (:c),
   :ops
   [{:index 1,
     :time 1,
     :process 0,
     :type :ok,
     :f :poll,
     :value [[:poll {:x [[1 :a] [2 :b]]}]]}
    {:index 7,
     :time 7,
     :process 0,
     :type :ok,
     :f :poll,
     :value [[:poll {:x [[4 :d]]}]]}]})}
```

`:int-poll-skip` finds the same thing, but inside a single operation. This is
helpful for detecting anomalies that could occur inside a single transaction.
For instance, this error shows that on key `x`, a single transaction polled values `:a` and `:d` in sequence, which skipped over `:b`.

```clj
{:int-poll-skip
 ({:key :x,
   :values [:a :d],
   :delta 2,
   :skipped (:b),
   :op
   {:index 3,
    :time 3,
    :process 0,
    :type :ok,
    :f :poll,
    :value [[:poll {:x [[1 :a] [4 :d]]}]]}})}
```

`:nonmonotonic-poll` finds cases where a single consumer, without changing its
assign/subscribe mapping for some key, performed subsequent calls to `poll` and
observed values in the second poll which started at or before the previous poll
finished. For instance, this error shows two successive operations on the same consumer which polled values `:c` then `:b`, jumping back one index.

```clj
{:nonmonotonic-poll
 ({:key :x,
   :values [:c :b],
   :delta -1,
   :ops
   [{:index 3,
     :time 3,
     :process 0,
     :type :ok,
     :f :poll,
     :value [[:poll {:x [[1 :a] [2 :b] [3 :c]]}]]}
    {:index 7,
     :time 7,
     :process 0,
     :type :ok,
     :f :poll,
     :value [[:poll {:x [[2 :b] [3 :c] [4 :d]]}]]}]})}
```

`:int-nonmonotonic-poll` does the same, but inside a single operation. For
instance, this error shows that on key `:x`, a single process went from reading offset 3 (`:c`) to offset 1 (`:a`), jumping two indices backwards in the log.

```clj
{:int-nonmonotonic-poll
 ({:key :x,
   :values [:c :a],
   :delta -2,
   :op
   {:index 3,
    :time 3,
    :process 0,
    :type :ok,
    :f :poll,
    :value [[:poll {:x [[3 :c] [1 :a]]}]]}})}
```

`:int-send-skip` looks for a single transaction which performs two subsequent
sends to the same key, and some other offset lands *between* those writes. This
is another way to detect G0 cycles, where writes interleave with one another.
In this example, a single transaction wrote `:a` and `:c` to key `:x`, skipping
over message `:b` from another transaction. This shows a lack of write
isolation between Kafka/Redpanda transactions, and appears to be normal
behavior.

```clj
{:int-send-skip
 ({:key :x,
   :values [:a :c],
   :delta 2,
   :skipped (:b),
   :op
   {:index 1,
    :time 1,
    :process 0,
    :type :ok,
    :f :send,
    :value [[:send :x [1 :a]] [:send :x :c]]}})},
```

`:nonmonotonic-send` finds cases where two subsequent operations on the same
producer sent values to the same key, and the latter message wound up at an
offset *prior* to the former message. For instance, this case shows that on key
`:x`, two calls to `send` on the same producer, split across two different
operations, wrote offsets out of order. The second operation's first send of
`:a` landed three indices before the first operation's final send of `:d`.

```clj
{:nonmonotonic-send
 ({:key :x,
   :values [:d :a],
   :delta -3,
   :ops
   [{:index 1,
     :time 1,
     :process 0,
     :type :ok,
     :f :send,
     :value [[:send :x [3 :c]] [:send :x [4 :d]]]}
    {:index 5,
     :time 11,
     :process 0,
     :type :ok,
     :f :send,
     :value [[:send :x [1 :a]] [:send :x [2 :b]]]}]})}
```

`:int-nonmonotonic-send` is the same thing, but inside a single transaction.
Here, two calls to send within a single transaction received offsets out of
order on key `:x`.

```clj
{:int-nonmonotonic-send
 ({:key :x,
   :values [:c :a],
   :delta -1,
   :op
   {:index 1,
    :time 1,
    :process 0,
    :type :ok,
    :f :send,
    :value [[:send :x [3 :c]] [:send :x [1 :a]]]}}
```

`:duplicate` occurs when a single value appears at multiple offsets in some key's log. Since we only ever insert unique values, and do not (above the level of the Kafka producer's internal retries) ever retry, we expect that each value appear at most once per key. This example shows that on key `:x`, value `:a` appeared at two distinct offsets.

```clj
{:duplicate ({:key :x, :value :a, :count 2})}
```

`:unseen` reports the number of messages which were successfully committed but
have not appeared in any poll, as of the last poll in the history. This checker
cannot distinguish between a lost write vs one which is simply very delayed. To
mitigate this weakness, we try *really hard* to read everything at the end of a
test---but that's still not a guarantee that `:unseen` errors are truly lost.
Sure, they texted "omw" three hours ago and still haven't shown up to Show
Tunes at Sidetrack, but they might not be dead. Maybe they're watching a sixth
episode of Golden Girls and trying to figure out which high tops to wear. We
don't judge.

This example includes the time (in nanoseconds since the start of the test) of
the final unseen inference, a map `:unseen` of keys to the number of unseen
messages on each key, and a map `:messages` of keys to the specific messages
unseen. This test failed to observe one committed message on key 6, and 5 on
key 23.

```clj
{:unseen {:time 1256465220303,
          :unseen {6 1, 23 5},
          :messages {6 (311),
                     23 (360 361 362 363 365)}}}
```

`:G0` finds write cycles: a cluster of transactions such that each wrote some
message both before *and* after every other transaction in the cluster. A data
structure representation is included in the workload. Here, for instance, a
pair of transactions had a cycle where T1 wrote before T2 on key `:x`, and
vice-versa on key `:y`. The `:cycle` key shows the operations involved: [T1,
T2, T1]. The `:steps` field explains the relationships between successive pairs
in that cycle. The first relationship was a write-write (`:ww`) dependency on key `:x`, where the first transaction wrote message `:a` and the second wrote message `:b`.

```clj
{:G0
 [{:cycle
   [{:index 2,
     :time 2,
     :process 0,
     :type :ok,
     :f :send,
     :value [[:send :x [0 :a]] [:send :y [1 :a]]]}
    {:index 3,
     :time 3,
     :process 1,
     :type :ok,
     :f :send,
     :value [[:send :x [1 :b]] [:send :y [0 :b]]]}
    {:index 2,
     :time 2,
     :process 0,
     :type :ok,
     :f :send,
     :value [[:send :x [0 :a]] [:send :y [1 :a]]]}],
   :steps
   ({:type :ww,
     :key :x,
     :value :a,
     :value' :b,
     :a-mop-index 0,
     :b-mop-index 0}
    {:type :ww,
     :key :y,
     :value :b,
     :value' :a,
     :a-mop-index 1,
     :b-mop-index 1}),
   :type :G0}]}
```

This can be a bit hard to understand from the data structure representation,
but you'll find corresponding plain-English and visual diagrams explaining this
cycle in `elle/G0.txt` and `elle/g0/`.

`:G1c` finds circular information flow: clusters where a cycle exists composed
of write-read and write-write dependencies. As in G0, write-write dependencies
are inferred from offsets written. Write-read dependencies are inferred
whenever one transaction polls another's sent messages. This G1c is comprised
entirely of write-read dependencies---note the `:wr` types in each step. The
first transaction sent `:a` to key `:x`, which was read by the second
transaction. The second transaction sent `:b` to key `:y`, which was read by
the first. Like G0, textual and visual explanations of this anomaly are
available in the `elle/` directory.

```clj
{:G1c
 [{:cycle
   [{:index 2,
     :time 2,
     :process 0,
     :type :ok,
     :f :txn,
     :value [[:send :x [0 :a]] [:poll {:y [[0 :b]]}]]}
    {:index 3,
     :time 3,
     :process 1,
     :type :ok,
     :f :txn,
     :value [[:send :y [0 :b]] [:poll {:x [[0 :a]]}]]}
    {:index 2,
     :time 2,
     :process 0,
     :type :ok,
     :f :txn,
     :value [[:send :x [0 :a]] [:poll {:y [[0 :b]]}]]}],
   :steps
   ({:type :wr, :key :x, :value :a, :a-mop-index 0, :b-mop-index 1}
    {:type :wr, :key :y, :value :b, :a-mop-index 0, :b-mop-index 1}),
   :type :G1c}]}
```

Note that both G0 and G1c cycles involving write-write edges may or may not be
"real" depending on how you interpret Kafka's `producer.send` semantics. If you
prefer to ignore these write dependencies, pass `--no-ww-deps` to the test, and
it'll *only* infer write-read edges.

Queue tests also generate an additional file, `consume-counts.edn`. This file
attempts---perhaps poorly---to tell whether a history offered "exactly once
semantics". It looks at all successful operations which performed a poll
operation while using `subscribe` (not `assign`, which we expect leads to
duplicate polls!), and counts the number of times each value was polled. Under
exactly-once semantics, I think this number should always be 1, but we were
never able to get this to work.

This file has two keys. `:distribution` is a map of counts (i.e. how many times
a record was polled) to the number of times that count occurred. `:dup-counts`
is a map of keys (topic-partitions) to values to the number of times that value
was polled, for any values which were polled multiple times. For examples, this test had 17133 messages which were polled once, and 174 which were polled twice. Key 2 had a single duplicate, message 79, which was saw twice.

```clj
{:distribution {1 17133, 2 174},
 :dup-counts
 {2 {79 2},
  3
  {111 2,
   112 2,
   113 2,
   114 2,
   ...}
 ...}}
```


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
