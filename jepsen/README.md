# Jepsen tests

![Experimental](https://img.shields.io/badge/status-experimental-orange)

> [!WARNING]
>
> This is a new, experimental test suite for data storage software. Passing it
> does not establish production safety. Review its coverage and contribute
> workloads for more failure cases to build confidence in the storage behavior.

[Jepsen](https://jepsen.io) tests for [`slap_streams`](../slap_streams/),
[`slap_yjs`](../slap_yjs/), [`slap_kv`](../slap_kv/),
[`slap_files`](../slap_files/), and
[`slap_snapshot_log`](../slap_snapshot_log/) on five nodes in Docker. Each node
runs a `jepsen_node` release built from `node/`. That release starts
[`slap`](../slap/)'s Durable Streams and KV listener against a shared RustFS
bucket, plus small HTTP APIs for Yjs documents and files (bodies in the same
bucket). Jepsen runs on the host and drives the nodes with `docker exec`.

An application sends requests to one of those nodes. A request may be
interrupted while the nodes change shard ownership or write to storage. The
suite checks whether the replies clients receive still fit each API's rules
through those faults.

## What Jepsen is

Jepsen can record a set of real operations that violates a specified rule. A
passing run means its particular operations, faults, and observations did not
show a violation of the rules its checkers applied. It does not cover every
possible execution. [Jepsen describes this limit
explicitly](https://jepsen.io/analyses/ethics).

Jepsen is a framework for experiments on real distributed systems. Its
controller runs outside the cluster under test. The controller does four jobs:

| Part          | Job in this suite                                                                                      |
|---------------|--------------------------------------------------------------------------------------------------------|
| **Generator** | Chooses which operation each client should attempt and when.                                           |
| **Client**    | Converts an operation into an HTTP request to a Slap node and records the result.                      |
| **Nemesis**   | Introduces faults, such as a network partition or a killed node, and later heals them.                 |
| **Checker**   | Reads the completed operation history and asks whether the observed results fit the promised behavior. |

A **history** is a time-ordered record of operation starts, operation results,
and fault events. Consider this history for one initially empty stream:

| Time | Client A                | Client B        |
|------|-------------------------|-----------------|
| 1    | Starts `append(7)`      |                 |
| 2    | Gets a successful reply |                 |
| 3    |                         | Starts `read()` |
| 4    |                         | Gets `[]`       |

Under a linearizable stream contract, this is impossible. Client B started
after A received success, so its read must include `7`. If the two operations
overlapped, the checker could consider more than one order. That real-time
constraint is the key idea behind
[linearizability](https://jepsen.io/consistency/models/linearizable).

The controller calls an operation `:ok` when it succeeded, `:fail` when it
definitely did not take effect, and `:info` when its outcome is uncertain. For
example, a write can commit and then lose its HTTP reply. A timeout cannot
prove that the write failed. The checker must consider both possible outcomes
of that `:info` write. An uncertain write does **not** by itself make the
whole run inconclusive; later reads can sometimes determine what happened.

### A small Clojure reading guide

Most of the controller is Clojure. These forms appear throughout the suite:

```clojure
{:type :invoke, :f :txn, :value [[:append 3 7]]}
```

`{...}` is a map. A name starting with `:` is a keyword used as a map key or
operation name. `[...]` is a vector. Here Jepsen is invoking a transaction
(`:txn`) containing one list-append action: append value `7` to key `3`.
Although Elle calls it a transaction, this suite makes each such transaction
exactly **one** read or append. It is not testing an atomic transaction across
multiple keys.

The client turns that invocation into a completion by returning a copy with a
new `:type`. For example, this is from
[`append.clj`](src/jepsen/slap/append.clj):

```clojure
:append (do (append! url v)
            (assoc op :type :ok))
:r      (assoc op :type :ok, :value [[:r k (read-all url)]])
```

`assoc` returns the operation map with fields replaced. A successful append
keeps its input value and becomes `:ok`; a successful read also records the
list returned by the server. The `defrecord Client` surrounding this code
implements Jepsen's client interface. `reify checker/Checker` elsewhere
defines a checker. `checker/compose` combines checkers so the test can apply
both a consistency rule and extra coverage checks.

## How a trial runs

### Nodes and storage

The test runs on five Docker nodes, `n1` through `n5`. Each node runs the
`jepsen_node` release built from this checkout. The release starts Slap's
stream and KV server plus small HTTP adapters used only by the Jepsen
workloads:

| Port | API used by the test |
| --- | --- |
| 4437 | Durable Streams and KV through the Slap server |
| 4438 | Yjs document operations, readiness, and statistics |
| 4439 | File operations and the final file audit |
| 4440 | Snapshot-log operations and follower checks |

All nodes share a RustFS object store. Each trial gets a new prefix in its
bucket so one trial's data does not become the next trial's initial state.
The Jepsen controller sends client requests to node IP addresses. For process
and network controls it runs commands inside the containers using
[`docker exec`](src/jepsen/slap/docker.clj). The node configuration is in
[`application.ex`](node/lib/jepsen_node/application.ex); controller startup,
kill, pause, and readiness behavior is in
[`db.clj`](src/jepsen/slap/db.clj).

Before the main workload, each node's `/ready` endpoint checks that all five
Erlang nodes can see each other and agree on the owners of all stream and KV
shards. A **shard** is a portion of data with one assigned writer. Agreement
on shard ownership is a setup condition, not a substitute for the later
correctness checks.

### Package coverage

The workloads cover the eight packages at these boundaries:

| Package | Jepsen path |
| --- | --- |
| `slap_slatedb` | All workloads use its durable storage; ownership changes exercise fencing indirectly. |
| `slap_cluster` | Every workload runs under both object leases and distributed placement. |
| `slap_streams` | `append`, `snapshot-log`, and `yjs` use durable streams. |
| `slap_snapshot_log` | `snapshot-log` checks snapshots, trims, follower resets, and reads; `yjs` uses it for document storage. |
| `slap_yjs` | `yjs` checks acknowledged adds, array contents and order, and compaction. |
| `slap_kv` | `kv` and `files` check conditional writes and final reads. |
| `slap_files` | `files` checks inline and object bodies, deletes, and cleanup. |
| `slap` | The node release serves the stream and KV HTTP APIs. |

Raw SlateDB calls and cluster placement internals have no separate Jepsen
client. Their direct checks live in their Mix projects; Jepsen tests their
effects through the higher-level APIs.

### One run, in order

The key controller code is in
[`jepsen.slap/slap-test`](src/jepsen/slap.clj):

```clojure
:generator (gen/phases
             (->> (:generator workload)
                  (gen/stagger (/ (:rate opts)))
                  (gen/nemesis (:generator nemesis))
                  (gen/time-limit (:time-limit opts)))
             (gen/log "Healing the cluster")
             (gen/nemesis (:final-generator nemesis))
             (gen/log "Waiting for recovery")
             (gen/sleep (:recovery-time opts))
             (gen/clients (:final-generator workload)))
```

Read `gen/phases` from top to bottom:

1. Generate client operations at an average rate while the nemesis injects
   faults. `--time-limit` bounds this phase only.
2. Run the nemesis's final healing operations.
3. Wait for the configured recovery interval (20 seconds by default).
4. Run the workload's final reads, follower checks, or file audit.
5. Apply the composed checkers to the recorded history and write results.

Many requests can fail during a fault. A short history with few successful
reads might not reveal that an acknowledged write disappeared. Final reads
and audits force the recovered cluster to expose its state. The checkers also
verify that those observations actually completed; otherwise the verdict is
`:unknown`.

The controller chooses one workload per trial. The workloads do not run
simultaneously in one history. It also records performance graphs and
unhandled exceptions; those sit beside the workload's correctness checker.

## Faults

`--nemesis` takes a comma-separated list, or `none`:

- `partition`: one node, a majority, or a ring of majorities is cut off
  from the others with iptables. RustFS stays reachable from every node.
- `kill`: one node or a minority is killed (SIGKILL) and later restarted.
- `pause`: one node or a minority is paused (SIGSTOP) and later resumed.
- `object-store`: RustFS is paused and later resumed, so every node loses
  access to the shared store during the fault. Store transitions are at least
  twice `--nemesis-interval` apart to give writes time to resume.

Clock faults are not available: containers share the host's clock. CI runs
separate trials for partition, kill, pause, and object-store for every workload
and placement strategy, with two trials of each combination.

The [`fault-checker`](src/jepsen/slap.clj) requires at least one completed
occurrence of each requested fault followed by a completed recovery event. If
the requested fault never happened, the run is `:unknown`, even if the data
checker found no problem.

## Placement

`--placement` picks the shard ownership strategy. `object-lease` is the
default and uses a six-second lease TTL. Placement changes which observations
are promised:

| Placement | Meaning for the checks |
| --- | --- |
| `object-lease` | A lease in the object store controls shard ownership. Stream and snapshot-log reads are checked against real-time order; KV and file register reads are checked for linearizability throughout the run. Acknowledged Yjs additions must appear in later reads. |
| `distributed` | Nodes infer placement from connectivity. A node with stale ownership information may temporarily serve an old read during faults. Stream and snapshot-log histories are checked for [serializability](https://jepsen.io/consistency/models/serializable), which does not require real-time visibility. KV and file reads are omitted during the fault phase, then required after ownership converges. Yjs is checked for lost additions and array consistency without the real-time visibility requirement. |

This difference is deliberate. If the test used a stronger rule than the
placement strategy claims, it could report expected stale reads as bugs. If
it used a weaker rule than claimed, it could miss real violations. In both
modes, the final observations provide a separate check that acknowledged
data is still present after recovery.

## Workloads

Each section describes the requests, the checks, and what a passing result
supports.

| Workload | Client operations | Main question |
| --- | --- | --- |
| `append` | Append a unique integer to a stream; read its full list. | Can Elle place the observed lists in an allowed order? |
| `kv` | Read, write, or compare and set a key. | Can Knossos explain each key as an atomic register? |
| `files` | Read, replace, conditionally replace, or delete a file. | Do register results, body shape, and the cleanup audit agree? |
| `snapshot-log` | Append an integer; reconstruct a list from a snapshot and later entries. | Do lists obey the selected ordering rule, and do followers catch up? |
| `yjs` | Insert a unique integer into an array; read the stored array. | Are acknowledged additions retained and observed arrays consistent? |

### Durable Streams: the `append` workload

**State under test.** Each key names a JSON stream. An append sends a JSON
array containing one unique integer. A read fetches every message from the
start of that stream, following `Stream-Next-Offset` until the server says
`Stream-Up-To-Date`. A missing stream reads as an empty list.

**What the client actually sends.** In
[`append.clj`](src/jepsen/slap/append.clj), `append!` posts `[v]` to the
stream endpoint. If the endpoint returns 404, the client creates the stream
and tries the append again. `read-all` repeatedly issues `GET` with an
`offset` query parameter. A successful reply to append becomes `:ok`; a 503
or timeout becomes `:info`, because the append may have committed without a
reply. A failed read contributes no observed list.

**How operations are chosen and checked.** The workload uses Jepsen's Elle
list-append generator:

```clojure
(append/test {:key-count          (:key-count opts)
              :min-txn-length     1
              :max-txn-length     1
              :max-writes-per-key (:max-writes-per-key opts)
              :consistency-models (consistency-models opts)})
```

The transaction length of one matters: each transaction touches one stream
with one read or one append. With object leases,
`consistency-models` selects `:strict-serializable`; for these independent,
single-operation streams, the practical claim is that each stream is
linearizable. With distributed placement it selects `:serializable`, which
permits a stale read to appear earlier in a legal ordering. Elle uses the
unique values and lists returned by reads to infer order constraints. For
example, reading `[4, 9]` says append `4` must precede append `9`; reading a
duplicate or a list that cannot fit any allowed order is evidence of a bug.

After recovery, the
[`list/final-generator`](src/jepsen/slap/list.clj)
asks every client thread to read every key touched during the trial. Its
checker verifies that each read was both scheduled and completed. This makes
missing final observations inconclusive instead of silently passing.

**What a pass supports.** The observed stream lists fit the selected ordering
rule, and every client could read each touched stream after recovery. A pass
does not establish behavior for producer idempotency, long polling, or
multi-stream atomic transactions; this workload does not issue those
operations. The official
[Durable Streams conformance suite](../slap/README.md#durable-streams-conformance-and-benchmarks)
covers additional stream operations.

### KV: the `kv` register workload

**State under test.** Each key is a row in its own KV partition, so keys can
land on different shard owners. The logical value is a single integer from
`0` through `4`. A missing row is the empty register. Clients randomly read,
write, or compare and set (CAS) keys. With object leases, reads are included
while faults run. With distributed placement, the active operations are
writes and CAS; reads resume after recovery.

**What a CAS actually does.** The client does not send an opaque CAS command.
It first reads the row and its ETag version. If the value is the expected old
value, it sends a conditional `PUT` using that version:

```clojure
(defn cas!
  [url [old new]]
  (let [current (fetch url)]
    (and current (= old (:value current))
         (put! url new {"If-Match" (:etag current)}))))
```

This is the relevant decision in
[`kv.clj`](src/jepsen/slap/kv.clj); the actual function also tags an exception
from `fetch` as a failure before any write. A CAS on a missing row fails
without writing. If the value did not match or the conditional `PUT` returned
HTTP 412, CAS is `:fail` and changed nothing.
A 503 or timeout after a `PUT` is `:info`. The client also rejects a successful
GET whose body is not exactly one digit from `0` to `4`; a corruption checker
fails the run if that happened.

**What Knossos checks.** Each key is fed to
`checker/linearizable` with `model/cas-register` through
`independent/checker`. Knossos asks whether some legal single-register
sequence could explain the observed reads, writes, and CAS results while
respecting real-time order. Suppose a row contains `0` and two concurrent
clients both attempt `CAS 0 → 1` and `CAS 0 → 2`. Both may read `0`, but
both conditional writes cannot legitimately succeed against the same ETag.
If the history says both succeeded, the register model rejects it.

Before final reads, [`register/final-generator`](src/jepsen/slap/register.clj)
waits for the `/ready` endpoint to report converged ownership. It then asks
every client to read every touched key. The final-read checker verifies that
all these reads completed. A complete final read can reveal a missing last
acknowledged value.

**What a pass supports.** The observed per-key operations are consistent with
an atomic register, no malformed value was observed, and every client could
read touched keys after recovery. The test does not check an atomic operation
across two keys, scans, or all possible value sizes. If an intermediate write
is overwritten before any read, the final value alone cannot show whether
that intermediate version survived a crash; Jepsen only judges what this
history made observable.

### Files: the `files` workload and cleanup audit

**State under test.** A file is treated as a CAS register with a body.
Writing `nil` deletes the file. A non-nil write picks either a one-byte body
or a 20,000-byte body, so successful writes exercise inline storage and
object-backed storage. The logical register value is the repeated digit,
while the body size chooses a storage path. The nodes keep old bodies for two
seconds and expire uploads after ten seconds, so cleanup runs during the test.
The client checks that a read body has a valid length and repeats a single
digit.

**How conditional writes work.** As in KV, CAS first reads the current value
and ETag. A non-nil write uses `PUT`; a deletion uses `DELETE`. A CAS from an
absent file to an absent file succeeds without a write. Changing an existing
file uses `If-Match`; creating an absent file uses `If-None-Match: *`. HTTP 412
means the condition failed. HTTP 409 means an upload expired before it could be
applied. Both become a definite `:fail`.
A write whose outcome is uncertain becomes `:info`.
Each `PUT` also carries a fresh `X-Jepsen-Write-Id` in metadata, so two
separate operations with the same body are not collapsed by the file API's
idempotent-write behavior. See
[`files.clj`](src/jepsen/slap/files.clj) and the
[`FilesRouter`](node/lib/jepsen_node/files_router.ex).

**Several checks run.** The Clojure workload composes its register
model with independent body and cleanup checks:

```clojure
:checker (checker/compose
           {:registers   ...
            :corruption (corruption-checker)
            :audit      (audit-checker)
            :final-reads (register/final-read-checker)})
```

The `...` stands for the Knossos checker and per-key timeline in the
full source. Knossos applies the same per-file CAS-register model as KV. The
corruption checker separately fails if a read returned a malformed body.
After all clients finish their final reads, the suite calls `POST /audit`.
The node-side audit does this, in order:

```elixir
with :ok <- on_every_node(:close_writes, [true], deadline),
     :ok <- quiesce(deadline),
     :ok <- settle(deadline),
     :ok <- barrier_intents(deadline),
     :ok <- on_every_node(Sweeper, :sweep, [], deadline),
     :ok <- on_every_node(Sweeper, :reconcile, [], deadline),
     {:ok, report} <- observe_until(deadline) do
  report
end
```

This excerpt from
[`files_router.ex`](node/lib/jepsen_node/files_router.ex)
omits the final statistics call but preserves the audit sequence. It first
refuses new writes on every node and waits for already-running handlers.
`settle` waits past upload deadlines. `barrier_intents` establishes a
linearizable view of intent partitions. Sweeping and reconciliation perform
the cleanup under test. `observe_until` then compares file records, object
bodies, upload intents, and object registrations.

The audit reports **false** for a file pointing to a missing object, an
orphaned registered object, a leaked registration, or an intent that remains
after cleanup. It reports **unknown** if it could not establish a quiescent
view or finds an unregistered object that a late store request may still have
written. It also requires evidence that both inline and object-backed writes
succeeded.

**What a pass supports.** File reads, writes, deletes, and CAS results fit
the register model; observed bodies had a valid length and repeated digit;
and the settled store had the checked relationships among records, objects,
intents, and registrations. The register model records the digit but not the
body length, so it cannot tell whether a read returned the other valid length
for the same digit. The workload does not test arbitrary file sizes or
selective failures of individual object-store requests.

### Snapshot logs: the `snapshot-log` workload

**State under test.** Each key is an append-only list of integers stored by
`Slap.SnapshotLog`. A snapshot replaces a prefix of the log. A fresh reader
must reconstruct the same logical list by reading the current snapshot and
then all entries after it. A follower on each node does that repeatedly and
publishes its accumulated list as a new snapshot after every five entries.
Other followers may publish competing snapshots and trim entries the first
follower has not yet read, causing it to reset from the newer snapshot.

**The Clojure client and node adapter.** The client in
[`snapshot_log.clj`](src/jepsen/slap/snapshot_log.clj)
turns an Elle append into `POST /logs/:key` and an Elle read into
`GET /logs/:key`. The node adapter in
[`log.ex`](node/lib/jepsen_node/log.ex)
calls `SnapshotLog.append` and reconstructs reads with
`SnapshotLog.next`. A returned read list is written back into Jepsen's
history as `[[:r k list]]`. A failed append response is uncertain: the
entry may still have been stored.

**Why there are two checkers.** Elle checks the lists returned by ordinary
reads under the same placement-dependent rule as `append`. The workload
also runs an explicit recovery sequence:

```clojure
:final-generator (gen/phases
                   (gen/once (fn [_ _]
                     {:type :invoke, :f :restore,
                      :value (sort @keys)}))
                   (gen/once (fn [_ _]
                     {:type :invoke, :f :check,
                      :value (sort @keys)}))
                   (list/final-generator keys))
```

`restore` starts followers for every touched key on all five nodes, including
nodes that restarted after a key stopped receiving operations. `check`
compares each follower's list with a fresh read of its log and waits for it
to catch up. A list that cannot be a prefix of the log is a **mismatch** and
fails. A missing or lagging follower, read error, or unreachable node makes
the result **unknown**. The final phase then has every client read every
touched log. The follower checker also requires a successful snapshot and
evidence of competing publication or a follower reset; otherwise the
snapshot-specific path was not exercised enough for a full pass. The
[`follower code`](node/lib/jepsen_node/log/follower.ex) and
[`LogRouter`](node/lib/jepsen_node/log_router.ex) implement these checks.

**What a pass supports.** Ordinary reads fit the selected list ordering
rule, the final readers completed, and all required followers caught up
without an observed lost, repeated, or reordered entry. This specifically
exercises snapshot publication, trimming, and recovery. It does not prove
that every future snapshot race is safe.

### Yjs: the `yjs` document workload

**State under test.** One Yjs document contains an array named `set`. Each
add gets a unique integer. The node adapter joins a document server, reads
its current Yjs state, inserts the new element at a random position, sends
the update, and waits for `Slap.Yjs.DocServer.sync/2` before returning
success. A read loads the document from durable storage and reconstructs
the array; it does not merely inspect the server's in-memory copy. See
[`yjs.ex`](node/lib/jepsen_node/yjs.ex).

**Generated operations.** This is the controller's
[`generator`](src/jepsen/slap/yjs.clj):

```clojure
(defn generator []
  (let [next-value (atom -1)]
    (fn []
      (if (< (rand) 0.25)
        {:f :read}
        {:f :add, :value (swap! next-value inc)}))))
```

About one quarter of operations are reads. Other operations add the next
unique integer. On the node, HTTP 200 means the update was confirmed
stored; 503 means it was not sent to the document server; 504 means it was
sent but not confirmed and is therefore indeterminate.

**What the checkers ask.** Jepsen's `set-full` checker tests membership.
With object leases, an acknowledged add must be in every read that starts
after it. Distributed placement does not require that immediate visibility.
A custom array checker keeps the raw array rather than converting it to a
set. It rejects duplicate elements, values that were never added, and an
earlier observed order that disagrees with the final array. For example, if
one read observed `[2, 1]`, a final `[1, 2]` changes the relative order of
those already observed elements. The checker also requires every client to
finish a final read, requires those final arrays to agree, and checks that
every acknowledged add is present. Another checker requires at least one
Yjs compaction, triggered by a low update-size threshold during the test.

**What a pass supports.** Acknowledged additions survived the observed
faults, immediate visibility held where object leases require it, no
duplicate or reordered observed element appeared, final arrays agreed, and
compaction actually ran. This is one Yjs document with one array and this
add/read pattern. It does not cover every Yjs data type or editing operation.

## Running

On Linux, install Docker Engine with Compose, JDK 21, Leiningen, gnuplot,
Graphviz, and `just`. The Nix development shell provides all of them except
Docker. Docker builds the node image.

From the repository root:

```sh
just jepsen-up        # build the node image and start the containers
just jepsen --workload append --time-limit 120
just jepsen --workload yjs --nemesis partition,pause --time-limit 300
just jepsen-down      # stop the containers
```

`just jepsen-test` runs the node's ExUnit tests and the controller's Clojure
tests. Without `just`, run the commands the recipes wrap, from the repository
root:

```sh
docker build -f jepsen/docker/Dockerfile -t slap-jepsen-node .
docker compose -f jepsen/docker/compose.yml up -d
(cd jepsen && lein run test --workload append --time-limit 120)
docker compose -f jepsen/docker/compose.yml down
```

For a longer local trial, run one workload and placement at a time:

```sh
just jepsen-up
just jepsen --workload snapshot-log --placement object-lease \
  --nemesis partition --time-limit 1800 --rate 10
just jepsen-down
```

For a multi-hour snapshot-log run, increase the writes allowed per key so the
final all-node follower audit remains tractable:

```sh
just jepsen --workload snapshot-log --placement object-lease --time-limit 10800 --rate 5 --max-writes-per-key 300
```

Retiring keys more often increases final audit work; retaining each key longer
increases its per-key history and snapshot activity. Review both the audit and
history-analysis time when choosing these settings.

`--time-limit` covers the operation phase; setup, recovery, final checks, and
history analysis take additional time. Use `--test-count 3` to repeat a trial
with fresh storage prefixes.

Repeat with `--nemesis partition`, `kill`, `pause`, or `object-store` to isolate
a fault. The default combines partition, kill, and pause. Review the final
checker verdict and the time spent analyzing the history; a completed
operation phase alone is not a passing trial.

`lein run test --help` lists the options (rate, shards, keys, fault interval).
The nodes use the subnet 10.47.0.0/24 (`docker/compose.yml`); the host must be
able to reach it directly.
Docker Desktop on macOS and Windows does not route to container IP addresses
from the host ([Docker networking](https://docs.docker.com/desktop/features/networking/networking-how-tos/)).
If your shell sets `HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY`, unset them and
their lowercase variants when running the test so requests reach the nodes
directly.

## Results

The top-level checker combines workload checks with fault coverage,
unhandled-exception detection, operation statistics, and graphs. In
`jepsen/store/<test name>/<timestamp>/`, start with `results.edn`:

| Verdict | What it means |
| --- | --- |
| `true` | The observed history satisfied every required checker and coverage gate for this trial. |
| `false` | At least one checker found an observed contradiction, such as two impossible CAS successes, a malformed file body, or a mismatched snapshot-log follower. |
| `:unknown` | The suite lacked a required observation or clean audit. Examples: a requested fault never completed, a final read failed, a follower did not catch up, or compaction never occurred. This is not evidence that the data was correct or incorrect. |

The `latest` symlink in each test directory points to its newest trial. Review
`results.edn` for the checker verdict and `history.txt` or `history.edn` for
operations and faults. `timeline.html` shows their timing;
`latency-quantiles.png`, `latency-raw.png`, and `rate.png` show latency and
operation rate. `jepsen.log` and `n1/jepsen_node.log` through
`n5/jepsen_node.log` contain controller and node logs. Register workloads also
write per-key results and timelines under `independent/`. Run `cd jepsen &&
lein run serve` to browse local results at `http://localhost:8080`. Manual runs
leave these files locally.

For `false`, inspect the violating history and node logs. For `:unknown`, first
find the fault, recovery check, final read, or audit that did not complete.

Each CI matrix job archives its `jepsen/store/` directory as
`jepsen-store.tgz` and uploads it to the Actions run as
`jepsen-<workload>-<placement>-<fault>`. The archive includes every trial (two
by default), with its graphs, histories, timelines, and logs. CI attempts the
archive and upload even when a test fails.

The CI matrix runs five workloads × two placements × four isolated faults ×
two trials: 80 trials. Each trial has 120 seconds of active operations.

## The node image

[`docker/Dockerfile`](docker/Dockerfile) builds the `node/` Mix release and
copies it into a smaller runtime image with the commands Jepsen runs on each
node. Build from the repository root because `node/` depends on the sibling
packages by path. The builder compiles `slap_slatedb`'s Rust NIF from source
and downloads `y_ex`'s prebuilt NIF. The build needs network access to fetch
Hex and Cargo dependencies and that NIF. The Mix and Cargo lockfiles pin
package dependencies, and the build checks that the Mix lockfile is current.
The Dockerfile pins its base images by digest.

Rebuild the image after changing any of the eight packages or `node/`. The
containers run as root so Jepsen can manage processes and network faults.

## Scope and limits

A test suite can only evaluate behavior it generates and observes. This one
uses five nodes, one shared RustFS instance, particular HTTP APIs, and the
faults above. It does not inject clock skew; all containers share the host
clock. Its object-store fault pauses RustFS globally; selective S3 request loss
and partial outages are not covered. It does not issue multi-key transactions,
arbitrary file operations, or every stream protocol operation. SlateDB fencing
and cluster placement are exercised indirectly through the higher-level APIs,
not by separate Jepsen clients.

The suite is strongest when a workload forces a bug to become visible in a
reply or final audit. Longer runs give faults and background processes more
chances to interact, but they do not expand what a checker can observe.
The correctness verdict does not promise a minimum availability or throughput:
operations may fail during faults, and the performance graphs are diagnostic
measurements rather than a benchmark gate.
Treat a passing run as concrete evidence for the tested behavior and a
failing history as a case to investigate and reproduce. The package tests,
model-based tests, Durable Streams conformance suite, and Jepsen experiments
answer different questions and complement one another.
