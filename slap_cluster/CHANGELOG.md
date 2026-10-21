# Changelog

## 0.4.0

- Adds `Slap.Cluster.status/2` to report the startup probe, shard lifecycle
  state, and retained failures, and `Slap.Cluster.await_ready/2` to wait for
  every shard to finish starting. Cluster modules expose `status/1` and
  `await_ready/1`.
- Readiness checks remote owners and direct shard children, including child
  restarts. It reports lifecycle state without accessing storage; it does not
  check current storage reachability or write durability.
- Timeouts cover the whole query or wait. Waits rediscover owners after
  placement changes, including when the timeout is `:infinity`.

## 0.3.0

- Distributed routing ignores placements from a killed strategy while the
  cluster retries its background store probe or awaits a new placement round.
- `probe: :background` starts the cluster before the store passes its probe.
  It retries after 1 s, backs off to at most 30 s, and opens shards only after
  a successful probe.
- `:settings` maps from a cluster module's `:defaults`, the application
  environment, and `start_link/1` are merged by key. Later sources override
  only the settings they contain.
- A strategy start failure is now reported under the `Slap.Cluster.Probe`
  child id instead of `:strategy`; the reason is unchanged.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_cluster`.
