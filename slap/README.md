# slap

Slap is a set of Elixir libraries that store streams, key-value data, files
and Yjs documents in S3-compatible object storage or a local directory, through
[SlateDB](https://slatedb.io). The services run inside your application rather
than as a separate database server. They run on one node or across several,
with each service's data split into shards that are spread across the nodes.

Add the `slap` package to install all seven Slap server libraries at once,
and start the services you use with one child spec, `{Slap, opts}`.

Start with the [getting started notebook](notebooks/getting-started.livemd)
to try the Elixir APIs in Livebook or IEx. To use Slap in an application,
follow the setup below. To try the HTTP APIs, run the
[standalone server](#try-the-standalone-server).

## Use Slap in an application

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later.

Add `slap` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap, "~> 0.7.1"}
  ]
end
```

Run `mix deps.get`.

Add `Slap` to your application's supervision tree, with a key for each
service to start:

```elixir
children = [
  {Slap,
   store: {:url, "s3://bucket/prefix"},
   streams: [shards: 8],
   kv: [shards: 8],
   files: [],
   yjs: []}
]
```

`streams:` and `kv:` start the Streams and KV clusters. `files:` requires
`kv:`, and `yjs:` requires `streams:`. Snapshot logs need only `streams:`.
See `Slap` for the options and the [architecture guide](guides/architecture.md)
for the services' dependencies and supervision trees.

You can also supervise each package's processes yourself, for example to
define your own cluster modules. Each package guide has setup instructions
and API examples:

| Package | Integration |
| --- | --- |
| [`slap_streams`](https://hexdocs.pm/slap_streams/) | Supervise `Slap.Streams.Cluster`; call `Slap.Streams` or serve `Slap.Streams.HTTP.Router` as a Plug. |
| [`slap_kv`](https://hexdocs.pm/slap_kv/) | Supervise `Slap.KV.Cluster`; call `Slap.KV` or serve `Slap.KV.HTTP.Router` as a Plug. |
| [`slap_files`](https://hexdocs.pm/slap_files/) | Supervise `Slap.KV.Cluster` before `Slap.Files`. |
| [`slap_yjs`](https://hexdocs.pm/slap_yjs/) | Supervise `Slap.Streams.Cluster` and `Slap.Yjs.Docs`. |
| [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) | Supervise `Slap.Streams.Cluster` before using the log. |
| [`slap_cluster`](https://hexdocs.pm/slap_cluster/) | Define shard children and supervise your module that uses `Slap.Cluster`. |
| [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/) | Open a database and use `Slap.SlateDB`. |

To serve HTTP, mount `Slap.Router` or an individual service's router in
your application's HTTP pipeline, or supervise a listener. Put your
authentication and authorization in front of the router. The
[server guide](guides/standalone-server.md#serving-the-apis-from-an-application)
shows the listener setup.

To explore the cluster's processes, messages exchanged during a write, and
stored data, run the
[Exploring Slap's internals](notebooks/internals.livemd) notebook in
Livebook.

## Kitchen sink demo

Download and run the [kitchen sink demo](guides/kitchen-sink.md) to try all
seven libraries:

```sh
curl -fsSLo kitchen_sink.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/kitchen_sink.exs
elixir kitchen_sink.exs
```

The script installs `slap`, updates sample data once a second, and prints
commands for browsing it with [Slap Inspect](#slap-inspect). Data is held in
memory and lost when the demo exits. Press Enter to exit.

The [demo guide](guides/kitchen-sink.md) covers running from a checkout,
inspection, and options. For an interactive walkthrough of the Elixir APIs,
see the [getting started notebook](notebooks/getting-started.livemd).

## Try the standalone server

[`examples/server.exs`](https://github.com/mjrusso/slap/blob/main/slap/examples/server.exs) serves the Streams and KV HTTP
APIs from one listener. It does not authenticate requests. Download it and
run it:

```sh
curl -fsSLo server.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/server.exs
elixir server.exs
```

The server listens on `http://127.0.0.1:4437`. In another terminal, write and
read a KV row:

```sh
curl -X PUT --data-binary 'Ada' http://localhost:4437/v1/kv/people/a
curl http://localhost:4437/v1/kv/people/a
```

The data is in memory and is lost when the server stops. The
[standalone server guide](guides/standalone-server.md) covers Streams and KV
HTTP examples, S3 and local storage, and serving the APIs from an
application.

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

- [The development server, `mix slap.server`](guides/development.md#development-server)
- [Example checks](guides/development.md#examples)
- [Durable Streams conformance and benchmarks](guides/development.md#durable-streams-conformance-and-benchmarks)
- [Crash tests](guides/development.md#crash-test)
- [Streams HTTP load benchmark](guides/development.md#streams-load)
- [Cluster tests](guides/development.md#cluster-tests)

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
