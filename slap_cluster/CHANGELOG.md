# Changelog

## Unreleased

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
