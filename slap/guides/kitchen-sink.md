# Kitchen sink demo

The [kitchen sink demo](https://github.com/mjrusso/slap/blob/main/slap/examples/kitchen_sink.exs)
runs all seven server libraries with in-memory storage and prints
`slap-inspect` commands for browsing the cluster data.

## What the demo does

Every second, the demo updates streams and a fork, a snapshot log, a Yjs
document, KV rows, and inline and object-backed files. It publishes a log
snapshot every five ticks. During setup, it also attempts a KV write that
is rejected as a conflict.

A custom cluster, `Slap.Examples.CounterCluster`, stores a counter through
SlateDB to demonstrate using `slap_cluster` directly.

Data is lost when the demo exits. Press Enter to stop it. With standard
input closed, it keeps running until the Erlang VM is stopped.

## Run without a checkout

With Elixir 1.18 or later and Erlang/OTP 26.2 or later, download the script
and run it directly:

```sh
curl -fsSLo kitchen_sink.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/kitchen_sink.exs
elixir kitchen_sink.exs
```

Outside a Mix project, the script installs `slap` from Hex with
`Mix.install`.

## Run from a checkout

From the repository root:

```sh
nix develop
just deps slap
cd slap
mix run examples/kitchen_sink.exs
```

## Options

`--ticks` limits the tick count; `--interval` sets the interval in
milliseconds:

```sh
mix run examples/kitchen_sink.exs --ticks 10 --interval 250
```

After the last tick, the demo keeps running so you can inspect its data. Add
`--exit` to exit after the last tick instead.

The same options work with `elixir kitchen_sink.exs`.

## Inspect the demo

Keep the demo running. In a second terminal, run one of the `slap-inspect`
commands the demo prints.
[Install Slap Inspect](../README.md#slap-inspect) first if needed.

To use an inspector built from your checkout, run from the repository root:

```sh
nix develop
just install-inspector
```

This installs `slap-inspect` into `.nix-mix/escripts`, which `nix develop`
puts on `PATH`.

In the Fork view, the first pages show the messages the fork inherits from its
source; press `n` to reach its own updates. The inspector's README
[lists the other keys](https://hexdocs.pm/slap_inspect/readme.html#terminal).
