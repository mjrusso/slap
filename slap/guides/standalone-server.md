# Standalone server

[`examples/server.exs`](https://github.com/mjrusso/slap/blob/main/slap/examples/server.exs)
serves the Streams and KV HTTP APIs from one listener. It does not
authenticate requests. Use it to try the HTTP APIs.

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later. The SlateDB NIF is
precompiled for macOS and Linux (glibc and musl) on x86_64 and arm64. The first
run downloads it and compiles the dependencies; later runs reuse them.

Download the script and run it:

```sh
curl -fsSLo server.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/server.exs
elixir server.exs
```

The script installs `slap` and `bandit` with `Mix.install`. The server
listens on `http://127.0.0.1:4437`, with Streams under `/v1/stream/` and KV
under `/v1/kv/`. `GET /health` answers 200 once the listener is up. By
default the data is in memory and is lost when the server stops.

## Streams

With the server running, use another terminal for the HTTP examples below.
Create a JSON stream, then append to it. The JSON extension stores each element
of an appended array as a separate message.

```sh
curl -X PUT -H 'Content-Type: application/json' http://localhost:4437/v1/stream/events
curl -X POST -H 'Content-Type: application/json' \
  --data '[{"type":"created","id":1},{"type":"updated","id":1}]' \
  http://localhost:4437/v1/stream/events
```

Read the stream. The JSON messages are returned as an array.

```sh
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

Fork the stream at its current end, then append to the fork. The source still
contains only the first two messages.

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -H 'Stream-Forked-From: /v1/stream/events' \
  http://localhost:4437/v1/stream/events-fork
curl -X POST -H 'Content-Type: application/json' --data '{"type":"deleted","id":1}' \
  http://localhost:4437/v1/stream/events-fork
curl 'http://localhost:4437/v1/stream/events-fork?offset=-1'
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

## KV

Set three rows, get one, then scan a range of keys in the
`people` partition. The range stops before `c`.

```sh
curl -X PUT --data-binary 'Ada' http://localhost:4437/v1/kv/people/a
curl -X PUT --data-binary 'Grace' http://localhost:4437/v1/kv/people/b
curl -X PUT --data-binary 'Katherine' http://localhost:4437/v1/kv/people/c
curl http://localhost:4437/v1/kv/people/a
curl 'http://localhost:4437/v1/kv/people?gte=a&lt=c'
```

The scan returns JSON with keys and values encoded as unpadded base64url.

## Storage

`--store local:DIR` keeps the data in a local directory:

```sh
elixir server.exs --store local:./data
```

For S3, create the bucket and set `AWS_REGION` and credentials (such as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) in the server's environment.
Start the server with:

```sh
elixir server.exs --store s3:s3://my-bucket/my-app
```

See
[choosing the shard count](https://hexdocs.pm/slap_cluster/readme.html#choosing-the-shard-count)
for local-storage guidance and
[running another cluster on the same store](https://hexdocs.pm/slap_cluster/readme.html#running-another-cluster-on-the-same-store)
before running a script against a store an application is using. Existing
data requires its original shard count.

Use `--port PORT` to change the port. For shard counts, placement or other
service settings, edit the options passed to `Slap` in the script. See
`Slap` for the available options.

## Inspection

Slap Inspect requires a named Erlang node. Start the script with a name:

```sh
elixir --sname slap server.exs
```

Then connect with `slap-inspect --node slap@HOST`. See the
[inspector guide](https://hexdocs.pm/slap_inspect/readme.html#standalone-slap-server)
for details.

## Serving the APIs from an application

Add `{:bandit, "~> 1.12"}` alongside `slap` in your application's
dependencies, then supervise `Slap` and a listener for `Slap.Router`:

```elixir
children = [
  {Slap, store: {:url, "s3://bucket/db"}, streams: [shards: 8], kv: [shards: 4]},
  {Bandit, plug: {Slap.Router, streams: [], kv: []}, port: 4437, ip: :loopback}
]
```

`Slap.Router` takes each service's router options (see
`Slap.Streams.HTTP.Router` and `Slap.KV.HTTP.Router`). Include only the
services the listener should serve. You can also mount an individual
service's router directly.

Put authentication and authorization in front of the router.
`ip: :loopback` accepts connections from this machine only; without `ip:`,
Bandit listens on every interface.

## Migrating from Slap.Server

`Slap.Server` combined service startup and the HTTP listener. Replace it
with the two children shown above:

1. Add `bandit` to your dependencies. Move `:store`, `:streams`, `:kv`,
   `:strategy` and `:peers` from `Slap.Server` to `Slap`.
2. Move `:port` and `:ip` to the Bandit child. Move any `:http` options
   nested under `:streams` or `:kv` to that service's options in
   `Slap.Router`.
3. Before starting against existing data, preserve each cluster's path,
   shard count and child options as described below.

The old server always used the paths `"streams"` and `"kv"`. Each cluster
used the `:shards` and `:child_options` passed directly to `Slap.Server`,
defaulting to 8 shards and `[]`. It ignored these three keys in
`config :slap_streams, Slap.Streams.Cluster` and
`config :slap_kv, Slap.KV.Cluster`.

`Slap` uses application-configured values for keys omitted from `streams:`
or `kv:`. Set `path:`, `shards:` and `child_options:` explicitly in each
service's options to preserve the old values, or update its application
config to match. The path must still point to the existing data, and the
shard count must match the one recorded in the store.

If you used the installed `slap.server` Mix task, switch to running
`examples/server.exs` with `elixir`. The script uses the default paths and 8
shards per cluster. If you started the task with `--streams-shards` or
`--kv-shards`, set the same `shards:` in the script. The Mix task remains
available in this repository for development.

## From a checkout

This repository's tests and benchmarks use the `slap.server` Mix task,
which takes server options as flags. It is available in the checkout's
`:dev` and `:test` environments. See the [development guide](development.md).
