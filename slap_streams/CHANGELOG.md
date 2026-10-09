# Changelog

## 0.2.0

- Add a `[:slap, :streams, :wait, :registered]` telemetry event, emitted
  when a read with `:wait` starts waiting.
- When a write by the deleter fails, the log now shows the `Slap.SlateDB.Error` message instead of a `MatchError`.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_streams`.
