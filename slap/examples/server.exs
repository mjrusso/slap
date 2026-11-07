# Serves the Durable Streams and KV HTTP APIs from one listener:
#
#     elixir server.exs [--store memory|local:DIR|s3:URL] [--port 4437]
#
# The listener binds to 127.0.0.1 and does not authenticate requests. For
# s3:URL, the endpoint and credentials come from the AWS_* environment
# variables. The memory store (the default) loses its data on exit.
{:ok, _} = Application.ensure_all_started(:mix)

if is_nil(Mix.Project.get()) and not Mix.installed?() do
  Mix.install([{:slap, "~> 0.7.0"}, {:bandit, "~> 1.12"}])
end

{opts, args, invalid} =
  OptionParser.parse(System.argv(), strict: [store: :string, port: :integer])

port = Keyword.get(opts, :port, 4437)

store =
  case Keyword.get(opts, :store, "memory") do
    "memory" -> :memory
    "local:" <> dir -> {:local, dir}
    "s3:" <> url -> {:url, url}
    _other -> nil
  end

if args != [] or invalid != [] or store == nil or port not in 1..65_535 do
  IO.puts(:stderr, "usage: elixir server.exs [--store memory|local:DIR|s3:URL] [--port 4437]")
  System.halt(1)
end

children = [
  {Slap, store: store, streams: [], kv: []},
  {Bandit, plug: {Slap.Router, streams: [], kv: []}, port: port, ip: :loopback}
]

{:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

# The VM stops the slap_* applications on SIGTERM, then kills processes that
# are not in an application, such as this supervisor. Stopping it first
# closes the shards in order.
System.trap_signal(:sigterm, fn -> Supervisor.stop(supervisor, :normal, 60_000) end)

IO.puts("""
Durable Streams on http://127.0.0.1:#{port}/v1/stream/
KV on http://127.0.0.1:#{port}/v1/kv/
""")

Process.sleep(:infinity)
