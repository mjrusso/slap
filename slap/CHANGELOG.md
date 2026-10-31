# Changelog

## Unreleased

- Adds a kitchen sink demo (`examples/kitchen_sink.exs`) that runs every
  service with in-memory sample data and prints Slap Inspect commands.
- Serves atomic JSON KV batches at `POST /v1/kv/{partition}`. Configure the
  batch size limits and the limits on requests waiting for durability through
  `kv: [child_options: ...]`; see
  [Limits on pending work](../slap_kv/README.md#limits-on-pending-work) for
  the defaults and the responses to rejected requests.
- Breaking change: Streams stores written by 0.4.0 and earlier need an
  offline upgrade before this version starts. Stop every node, take a backup,
  and follow the
  [upgrade instructions](../slap_streams/README.md#upgrading-existing-stream-stores).
- Breaking change: the Streams options `:max_fork_copy_bytes` and
  `:fork_copy_grace` are replaced by `:max_fork_depth` and `:creation_grace`;
  see the [`slap_streams` changelog](../slap_streams/CHANGELOG.md).
- Breaking change: `slap_yjs` upgrades y_ex to 0.13, which changes how
  buffer values read; see the [`slap_yjs` changelog](../slap_yjs/CHANGELOG.md).
- Each service refuses to start when its shard count differs from the count
  its store was created with; see the
  [`slap_cluster` changelog](../slap_cluster/CHANGELOG.md).

## 0.4.0

- Updates `slap_cluster`, `slap_streams`, `slap_snapshot_log`, `slap_yjs`,
  `slap_kv`, and `slap_files` dependencies to 0.4.0.
- Breaking change for Yjs users: local awareness publishers must subscribe
  and claim their client ids before submitting awareness messages. See the
  `slap_yjs` changelog for the ownership requirements and multi-node limitation.

## 0.3.0

- Adds `slap_snapshot_log`, `slap_yjs`, and `slap_files` as dependencies.
  Installing `slap` now installs all seven library packages. The standalone
  server continues to serve Streams and KV.
- Because it now depends on `slap_yjs`, `slap` requires Erlang/OTP 26.2 or
  later.
- Service `:settings` now merge with the cluster defaults. `flush_interval`
  remains 10 ms unless explicitly set.

## 0.2.0

- `slap.server` runs without a Mix project, via `Mix.install`.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap`.
