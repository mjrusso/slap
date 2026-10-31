# Changelog

## Unreleased

- `config :rustler_precompiled, :force_build, slap_slatedb: true` now builds
  the NIF from source. Previously it had no effect, and only
  `SLAP_SLATEDB_BUILD=1` forced a build.
- Adds `get_range/3`, which reads at most `:max_value_bytes` of a value
  (optionally from `:value_range`) and returns the value's full size. It
  limits the bytes copied into the BEAM; SlateDB still loads the whole value
  in native memory.
- Adds `preview_iterator/2`, an iterator that cuts keys and values to
  `:max_key_bytes` and `:max_value_bytes` and returns their full sizes. It
  limits the bytes copied into the BEAM; SlateDB still loads whole rows in
  native memory.
- Local stores (`{:local, directory}`) support conditional updates
  (`{:update, version}`), so SlateDB's garbage collector removes obsolete
  metadata on every run, not only the first, and `slap_cluster`'s
  `ObjectLease` strategy works with a local store. Conditional updates return
  `:unsupported` on filesystems without microsecond modification times.
- Writes under one local store root now run one at a time. Before upgrading,
  stop processes running older versions. Open the same root in every process
  that uses the data, and do not delete the `.slap-lock#0` file in the root.
- Local stores reject recognized network filesystems, including NFS and
  SMB/CIFS.
- Local stores sync object writes and completed uploads to disk with `fsync`.
  On Unix, directory changes are synced too. Data survives OS crashes and power
  loss more reliably, and writes and opens can take longer.

## 0.2.0

- Upgrade SlateDB to 0.17. Databases written by 0.16 open unchanged.
- Keys can be up to 4 GiB long; the previous limit was 65,535 bytes.
- `Slap.SlateDB.Iterator.seek/3` returns an `:invalid` error for an iterator
  with `order: :desc`.
- SlateDB 0.17's new settings can be passed in `:settings`, among them
  `compactor_options.checkpoint_lifetime` and
  `compactor_options.scheduler_options.sorted_run_consolidation_threshold`.
- Ranged downloads: `Slap.SlateDB.ObjectStore.download/3` takes
  `range: {first, last}` (inclusive) and downloads only those bytes of the
  object.

## 0.1.0

Initial release of `slap_slatedb`.
