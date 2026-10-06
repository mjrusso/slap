# Agent instructions

Use direct, literal language.

## Commits

Use Conventional Commits with the package directory as the scope for package
changes, for example `feat(slap_streams): add stream storage`. Use
`test(jepsen)` for Jepsen suite changes, `ci` for GitHub Actions, and
`chore(repo)` for repository tooling and shared files. Keep each commit focused
on one package or one infrastructure concern.

This repository holds eight Mix projects, each with its own dependencies,
checks and CI workflow:

- `slap_slatedb/`: Elixir bindings for SlateDB (a Rust NIF).
- `slap_cluster/`: runs N SlateDB databases across nodes; depends on
  `slap_slatedb` by path.
- `slap_streams/`: the Durable Streams server, to embed; depends on both by
  path.
- `slap_snapshot_log/`: an append-only log with snapshots that replace its
  prefix, on `slap_streams`; depends on the three above by path.
- `slap_yjs/`: Yjs documents stored in Durable Streams (y_ex), as snapshot
  logs; depends on the four above by path. Needs OTP 26.2 or later.
- `slap_kv/`: a partitioned key-value store with conditional writes;
  depends on `slap_cluster` and `slap_slatedb` by path.
- `slap_files/`: files with bodies inline in `slap_kv` records or as
  objects in a store; depends on `slap_kv` and what it depends on, by path.
- `slap/`: the standalone server (`mix slap.server`) for streams and KV,
  with the Durable Streams conformance suite and benchmarks and the crash
  and cluster tests; depends on `slap_streams`, `slap_kv` and what they
  depend on, by path.

The path dependencies above are selected by `SLAP_LOCAL_DEPS=1`. With the
variable unset or set to `0`, the packages use versioned Hex dependencies.
Each project's `mix.lock` is committed, as `mix deps.get` writes it with
`SLAP_LOCAL_DEPS=1`. With `0`, `mix deps.get` adds the `slap_*` packages from
Hex to it; do not commit those entries (`git restore` the file).

`jepsen/` holds the Jepsen suite (Clojure, Leiningen) and `jepsen/node/`, the
Mix project each Jepsen node runs; see `jepsen/README.md`. Build the node
image from the repository root with `jepsen/docker/Dockerfile`. Rebuild it
(`just jepsen-up`) after changing any of the eight projects or `jepsen/node/`.

## Environment

`flake.nix` provides the tools: OTP 29, Elixir 1.20, Rust (with rustfmt and
clippy), Node with pnpm for the official Durable Streams conformance suite,
and Leiningen with a JDK for the Jepsen suite. Run Mix commands in
`nix develop` (or through direnv, with `use flake`). Jepsen can also run
without Nix; see `jepsen/README.md`. The shell keeps Mix and Hex in
`.nix-mix/` and `.nix-hex/`, and sets `SLAP_LOCAL_DEPS=1` and
`SLAP_SLATEDB_BUILD=1`, so sibling packages and the SlateDB NIF are built
from this checkout. `just deps`
installs Hex and fetches dependencies;
`just check [project]` runs `mix check` in every project, or in one, and
for slap_slatedb also the NIF's Rust checks (`just check-rust`: rustfmt,
clippy and `cargo test`, as CI runs them). `just smells [project]` reports
Reach's smell findings, which are advisory and not part of `mix check`.
`just format` formats the Nix, Rust and Elixir code.
Each package README has the same preamble, between `<!-- slap-preamble -->`
lines: edit `scripts/readme_preamble.md` and run `just readme-preamble`.
`just readme-preamble-check` (run by `just check`, `just lint` and CI) fails
if a copy differs.
`just streams-conformance` runs the Durable Streams conformance suite
against `mix slap.server`; run it after changing `slap_streams`' protocol
behaviour. `just streams-bench` runs the official Durable Streams benchmarks
against `mix slap.server` and the official Caddy server. `RELEASING.md`
describes releases, through `just release-status`, `just release-tags` and
`just publish`.

## Checks

Run `mix check` in every project you changed, and in the projects that
depend on it, before finishing a change. Fix every failure. `mix check`
runs, in the test environment: compilation with warnings as errors,
`mix format --check-formatted`, `mix deps.unlock --check-unused`, Credo
(with ExSlop and ExDNA), Reach's architecture check (`mix reach.check
--arch`, except in slap_slatedb), Dialyzer, a compile-time dependency cycle
check, and the tests.

- Run `mix format` to apply formatting.
- Keep findings from the enabled Credo and duplication checks at zero.
- Fix findings rather than weakening lint configuration.
- Each package's `.reach.exs` encodes invariants below as call rules: who
  opens and writes SlateDB databases, deletes and uploads objects, and what
  the HTTP layers may call. A new writer or caller changes an invariant:
  update the policy and its comment with it. Reach attributes the calls in
  a file with several modules to one of them, so keep a module that a rule
  names in its own file.
- Use `mix ex_dna` to inspect duplication groups, and `mix ex_dna.explain N`
  to inspect a particular group.
- Some tests need RustFS and are excluded without it
  (`SLAP_TEST_S3_ENDPOINT`, `SLAP_TEST_S3_BUCKET`). CI provides it;
  see `.github/workflows/`.

## Invariants

These keep acknowledged data safe. A change that breaks one needs a
reason in its commit message and a test.

- Never acknowledge or expose data that is not durable. A stream server
  replies to an append, and bounds every read, from its durable view. A KV
  partition writer replies to a write once it is durable, and to a
  conflict once the state it saw is durable and a write of its own confirms
  that its node still owns the shard; KV reads use SlateDB's `:remote`
  durability, except linearizable ones, which the partition writer answers
  like a conflict.
- One SlateDB writer per shard: the database handle of its one owner
  (slap_cluster's placement). SlateDB's fencing is the safety net for when
  placement is wrong, not the mechanism. On the owner, one process writes
  each stream (its stream server) and each KV partition (its shard's
  `Slap.KV.PartitionWriter` for it), all through that handle.
- The data, tail and producer state of one append go in one
  `Slap.SlateDB.write/3` batch.
- Offsets on the wire use the official server's format
  (`%016d_%016d`) and accounting (4 bytes plus the body per message).
- No `:timeout` on `slap_slatedb` writes: a timed-out write is not cancelled
  and may commit after later ones, so ordering holds only for writes that
  got a reply.
- A write error must not be ignored: a stream server or KV partition
  writer that fails a write stops (see `write_failed` in `slap_streams` and
  `slap_kv`), and a Yjs document server stops on an append error that is
  not transient. A process that takes over a stream or KV shard first
  waits until the writes before it are durable.
- A file's object body is named by an intent before it can become
  unreferenced (before it exists, for an upload), and its key is
  registered after that and before the upload. An object is deleted only
  by a sweep that took its intent, if no file points to it, and then
  unregistered; or by reconciliation, if it has no registration (it was
  written after a sweep deleted it, and no file can point to it).
  Reconciliation lists objects before registrations. A writer takes an
  upload's intent before pointing a file at its object. Opening an intent,
  and a record write or registration that it covers, have the intent's due
  time as their KV deadline (exclusive: a write is not applied at it); the
  sweeper acts `max_clock_skew_ms` after it, and reads the record
  linearizably.
- A snapshot log's snapshot at an offset contains every entry before it,
  and anything more only if applying it again is harmless. It becomes
  current through the index's `Stream-Seq`, and only then are entries
  trimmed; every step of a publication can be interrupted.
- `slap_cluster` stays generic: no stream, KV or Yjs code. Application
  behaviour goes in its shard children.
- A change to stream behaviour extends the model-based property test
  (`slap_streams/test/model_test.exs`), and the Durable Streams
  conformance suite stays green. A change to KV behaviour extends
  `slap_kv/test/model_test.exs`, and one to files
  `slap_files/test/model_test.exs`.

## Code

- Prefer pattern matching and function clauses over nested conditionals.
- Use `{:ok, value}` and `{:error, reason}` for expected failures.
- Do not raise exceptions for ordinary control flow.
- Give synchronous process calls an explicit timeout appropriate to the
  operation.
- Add abstractions when existing consumers need them.
- Remove dead code, unused dependencies, and redundant wrappers.
- Comments should explain constraints or invariants needed to change code
  safely. Avoid comments that narrate the code.

## Tests

- Write tests around observable behavior rather than implementation
  details.
- Prefer per-test ownership and `async: true`. Use synchronous tests for
  unavoidable shared state and document the reason.
- Start test processes with `start_supervised!/1`.
- Synchronize tests through messages, acknowledgements, or monitors. Do not
  use sleeps to coordinate new tests. (The multi-node failover tests poll
  for cluster states they cannot be told about.)
- Keep ordinary checks deterministic and credential-free.
- A regression test should fail without its fix: check that it does.

## Dependencies

- Consult dependency usage rules and documentation before using unfamiliar
  APIs.
- Commit the `mix.lock` changes that come with a dependency change.
- After changing the usage rules configuration, run
  `MIX_ENV=dev mix usage_rules.sync` in that project (it writes the
  project's own `AGENTS.md`).
