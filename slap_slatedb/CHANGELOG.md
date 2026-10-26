# Changelog

## Unreleased

- Fix repeated metadata garbage collection on local stores by supporting
  ETag-conditional updates. Local mutations serialize per store root. Stop old
  processes before upgrading, and use the same root in every process accessing
  the same data.

- Reject recognized network filesystems, including NFS and SMB/CIFS, for local
  stores.

- Local stores (`{:local, directory}`) now use `fsync` for object writes and
  completed uploads. On Unix, directory changes are synced too. This improves
  protection against OS crashes and power loss, and can increase write and
  open latency.

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
