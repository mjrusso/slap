# slap

Slap is a set of Elixir libraries that store streams, key-value data, files
and Yjs documents in S3-compatible object storage or a local directory, through
[SlateDB](https://slatedb.io). The services run inside your application rather
than as a separate database server. They run on one node or across several,
with each service's data split into shards that are spread across the nodes.

Add the `slap` package to install all seven Slap server libraries at once. It
also provides `mix slap.server`, a standalone HTTP server for evaluating and
benchmarking Streams and KV.

- [Run the kitchen sink demo](#kitchen-sink-demo) to see every service update
  sample data once a second.
- [Try the libraries](guides/library-examples.md) in IEx.
- [Run the standalone server](#try-the-standalone-server) and use its HTTP APIs.
- [Use Slap in an application](#use-slap-in-an-application) to embed the
  services.
- [Browse the data with Slap Inspect](#slap-inspect), a read-only TUI and CLI
  installed separately.

See the [package overview](guides/overview.md) for a description of each
package.

## Use Slap in an application

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later.

Add `slap` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap, "~> 0.5.1"}
  ]
end
```

Run `mix deps.get`.

Add the required processes to your application's supervision tree. Each
package guide has setup instructions and API examples:

| Package | Integration |
| --- | --- |
| [`slap_streams`](https://hexdocs.pm/slap_streams/) | Supervise `Slap.Streams.Cluster`; call `Slap.Streams` or serve `Slap.Streams.HTTP.Router` as a Plug. |
| [`slap_kv`](https://hexdocs.pm/slap_kv/) | Supervise `Slap.KV.Cluster`; call `Slap.KV` or serve `Slap.KV.HTTP.Router` as a Plug. |
| [`slap_files`](https://hexdocs.pm/slap_files/) | Supervise `Slap.KV.Cluster` before `Slap.Files`. |
| [`slap_yjs`](https://hexdocs.pm/slap_yjs/) | Supervise `Slap.Streams.Cluster` and `Slap.Yjs.Docs`. |
| [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) | Supervise `Slap.Streams.Cluster` before using the log. |
| [`slap_cluster`](https://hexdocs.pm/slap_cluster/) | Define shard children and supervise your module that uses `Slap.Cluster`. |
| [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/) | Open a database and use `Slap.SlateDB`. |

These packages do not start an HTTP listener. The Streams and KV HTTP routers
are Plugs: mount them in your application's HTTP pipeline, or serve them with a
listener you supervise, and put your authentication and authorization in front
of them.

## Kitchen sink demo

To try all seven libraries without creating an application, download and run
the [kitchen sink demo](guides/kitchen-sink.md):

```sh
curl -fsSLo kitchen_sink.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/kitchen_sink.exs
elixir kitchen_sink.exs
```

The script installs `slap`, updates sample data once a second, and prints
commands for browsing it with [Slap Inspect](#slap-inspect). Data is held in
memory and lost when the demo exits. Press Enter to exit.

The [demo guide](guides/kitchen-sink.md) covers running from a checkout,
inspection, and options. For an interactive walkthrough of the Elixir APIs,
see the [library examples](guides/library-examples.md).

## Try the standalone server

`mix slap.server` serves Streams, KV, or both from one HTTP listener. It does
not authenticate requests. Use it to evaluate, test and benchmark the services
without writing an application:

```sh
elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store memory
```

The server listens on `http://127.0.0.1:4437`. In another terminal, write and
read a KV row:

```sh
curl -X PUT --data-binary 'Ada' http://localhost:4437/v1/kv/people/a
curl http://localhost:4437/v1/kv/people/a
```

The memory store loses its data when the server stops. The
[standalone server guide](guides/standalone-server.md) covers Streams and KV
HTTP examples, S3 and local storage, server options, and supervision in a
release.

## Slap Inspect

[Slap Inspect](https://hexdocs.pm/slap_inspect/) is a read-only TUI and CLI
for browsing data in the standalone server or in your application. Install
it separately:

```sh
mix escript.install hex slap_inspect
```

It connects over distributed Erlang using the server's node name and cookie.
The [inspector guide](https://hexdocs.pm/slap_inspect/) covers connection and
commands. The [demo guide](guides/kitchen-sink.md#inspect-the-demo) explains
how to inspect the kitchen sink demo.

## Development and testing

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development) to work from
this checkout. The [development guide](guides/development.md) documents the
server's test and benchmark tools:

- [Library example checks](guides/development.md#library-examples)
- [Durable Streams conformance and benchmarks](guides/development.md#durable-streams-conformance-and-benchmarks)
- [Crash tests](guides/development.md#crash-test)
- [Streams HTTP load benchmark](guides/development.md#streams-load)
- [Cluster tests](guides/development.md#cluster-tests)

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
