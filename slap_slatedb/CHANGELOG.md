# Changelog

## Unreleased

- Upgrade SlateDB to 0.17. Databases written by 0.16 open unchanged.
- Keys can be up to 4 GiB long; the previous limit was 65,535 bytes.
- `Slap.SlateDB.Iterator.seek/3` returns an `:invalid` error for an iterator
  with `order: :desc`.
- SlateDB 0.17's new settings can be passed in `:settings`, among them
  `compactor_options.checkpoint_lifetime` and
  `compactor_options.scheduler_options.sorted_run_consolidation_threshold`.

## 0.1.0

Initial release of `slap_slatedb`.
