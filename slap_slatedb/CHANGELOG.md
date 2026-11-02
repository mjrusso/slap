# Changelog

## 0.3.0

- If using local storage (`{:local, directory}`), writes now share one
  exclusive OS lock per store root. Before starting this version, stop all
  processes accessing that store with older `slap_slatedb` versions, which do
  not take this lock. Use the same root in every process accessing the data,
  and keep the root's `.slap-lock#0` file.
- Local stores reject recognized network filesystems, including NFS and
  SMB/CIFS.
- Local stores sync object writes and completed uploads to disk with `fsync`.
  On Unix, they also sync directory changes. Persistence depends on the
  filesystem and device honoring sync requests. Writes and opens can take
  longer.
- Local stores support conditional updates (`{:update, version}`). This lets
  SlateDB's garbage collector remove obsolete metadata on subsequent runs and
  lets `slap_cluster` use `ObjectLease` on local storage. Conditional updates
  return `:unsupported` on filesystems without microsecond modification times.
- Fixes `config :rustler_precompiled, :force_build, slap_slatedb: true` being
  ignored. It now builds the NIF from source, as `SLAP_SLATEDB_BUILD=1` does.
- Adds `get_range/3` to preview a value with `:max_value_bytes` and optional
  `:value_range`, and `preview_iterator/2` to preview keys and values with
  `:max_key_bytes` and `:max_value_bytes`. Both return full sizes and limit
  bytes copied into the BEAM; SlateDB still loads whole rows in native memory.

## 0.2.0

- Upgrades SlateDB to 0.17. Databases written by 0.16 open unchanged.
- `Slap.SlateDB.Iterator.seek/3` returns an error with `kind: :invalid` for an
  iterator with `order: :desc`.
- Raises the maximum key size from 65,535 bytes to 4 GiB minus one byte.
- `Slap.SlateDB.ObjectStore.download/3` accepts `range: {first, last}`
  (inclusive) to download part of an object.
- SlateDB 0.17's new settings can be passed in `:settings`, among them
  `compactor_options.checkpoint_lifetime` and
  `compactor_options.scheduler_options.sorted_run_consolidation_threshold`.

## 0.1.0

Initial release of `slap_slatedb`.
