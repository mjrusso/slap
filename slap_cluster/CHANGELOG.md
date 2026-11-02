# Changelog

## 0.5.0

- Refuses to start when `:shards` differs from the store's recorded shard
  count, preventing records from being routed to the wrong shard and reading
  as missing. On the first start with this release, stores created by
  `slap_cluster` 0.4.0 or earlier are checked before the shard count is recorded;
  see [Choosing the shard count](README.md#choosing-the-shard-count).
  With `probe: :background`, a mismatch stops the cluster.
- Fixes database closure being interrupted when a shard is killed or exceeds
  its shutdown timeout. Closure now finishes independently, and a replacement
  shard on the same node waits for it before opening the database.
- Uses `slap_slatedb` 0.3.0. If using local storage (`{:local, directory}`),
  writes now share one exclusive OS lock per store root. Before starting this
  version, stop all processes running older Slap versions that access the same
  store; older versions do not take this lock.
  Writes to local object storage now sync to disk; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-3-0).
- `Slap.Cluster.Strategy.ObjectLease` works with a local store
  (`{:local, directory}`) shared by processes on one host. Network filesystem
  mounts remain unsupported.
- Adds read-only inspection of raw stored rows for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/). Reads use the shard owner's
  open database handle, return only durable data and limit reply sizes. Each
  node admits one inspection request at a time.
- Adds `Slap.Cluster.clusters/0` to list a node's clusters, shard counts, local
  shards and services. Shard children modules report services through the new
  optional `c:Slap.Cluster.ShardChildren.services/0` callback.

## 0.4.0

- Adds `Slap.Cluster.status/2` for startup probe results, shard lifecycle state
  and recorded failures, and `Slap.Cluster.await_ready/2` to wait for every
  shard to finish starting. Cluster modules expose `status/1` and `await_ready/1`.
- Readiness checks remote owners and direct shard children, including
  restarts. It does not check storage reachability or write durability.
- The timeouts of `status/2` and `await_ready/2` cover the whole call.
  `await_ready/2` rediscovers owners after placement changes, including when
  its timeout is `:infinity`.

## 0.3.0

- Distributed routing ignores placements from a killed strategy while the
  cluster retries its background store probe or awaits a new placement round.
- `:settings` maps from cluster defaults, the application environment and
  `start_link/1` are merged by key. Later sources override matching keys.
- `probe: :background` starts the cluster before the store passes its probe.
  It retries after 1 s, backs off to at most 30 s, and opens shards only after
  a successful probe.
- A strategy start failure is now reported under the `Slap.Cluster.Probe`
  child id instead of `:strategy`; the reason is unchanged.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_cluster`.
