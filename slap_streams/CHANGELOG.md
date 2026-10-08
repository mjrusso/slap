# Changelog

## Unreleased

- Add a `[:slap, :streams, :wait, :registered]` telemetry event, emitted
  when a read with `:wait` starts waiting.
- When a write by the deleter fails, the log now shows the `Slap.SlateDB.Error` message instead of a `MatchError`.

## 0.1.0

Initial release of `slap_streams`.
