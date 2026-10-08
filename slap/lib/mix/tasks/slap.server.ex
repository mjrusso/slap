defmodule Mix.Tasks.Slap.Server do
  @shortdoc "Runs Durable Streams, KV, or both"
  @moduledoc """
  Starts a standalone HTTP server (`Slap.Server`) for Durable Streams, KV,
  or both. Select at least one service. Applications can embed either service
  directly in their supervision tree and Plug pipeline.

      mix slap.server [--help] [--port 4437] [--ip 127.0.0.1]
                    --store memory|local:DIR|s3:URL
                    [--streams] [--kv]
                    [--streams-shards 8] [--streams-flush-interval 10ms]
                    [--streams-idle-timeout MS]
                    [--streams-max-fork-copy-bytes BYTES]
                    [--streams-fork-copy-grace MS]
                    [--streams-expiry-interval MS]
                    [--streams-repair-interval MS]
                    [--streams-deleter-interval MS]
                    [--streams-deleter-page N]
                    [--streams-load-interval MS]
                    [--streams-max-inflight-bytes-per-stream BYTES]
                    [--streams-max-inflight-bytes-per-shard BYTES]
                    [--long-poll-timeout 30000] [--sse-timeout 60000]
                    [--pid-file FILE]
                    [--placement local|object-lease|distributed|static]
                    [--lease-ttl MS] [--static-nodes name@host,...]
                    [--peers name@host,...]
                    [--kv-shards 8] [--kv-flush-interval 10ms]
                    [--kv-partition-writers N]

  For `s3:URL`, the endpoint and credentials come from the `AWS_*`
  environment variables. `--store` is required; a `memory` store is lost
  when the server stops.
  Streams are served under `/v1/stream/` and KV under `/v1/kv/`;
  `GET /health` answers 200 once the listener is up.
  Each service has its own shards and databases.
  `--pid-file` writes the VM's OS process id, for tests that kill it. There
  is no authentication. The listener binds to loopback unless `--ip`
  selects another address.

  Use `iex -S mix slap.server ...` to keep an interactive prompt while the
  listener runs.

  Without a Mix project, use `Mix.install`:

      elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \\
        --streams --store memory

  With a placement other than `local`, the node is one of a cluster: start
  it with a name and a shared cookie, for example `elixir --name
  a@127.0.0.1 --cookie slap -S mix slap.server --streams --store
  s3:s3://bucket/streams --placement distributed --peers
  b@127.0.0.1,c@127.0.0.1`. `object-lease` needs an S3 store;
  `--peers` requires a placement other than `local`.
  """

  use Mix.Task

  alias Mix.Tasks.Help
  alias Slap.Cluster.Strategy
  alias Slap.SlateDB

  @stream_child_switches for key <- Slap.Streams.Cluster.child_option_keys(),
                             do: {key, :"streams_#{key}"}

  @switches [
              help: :boolean,
              port: :integer,
              ip: :string,
              store: :string,
              streams: :boolean,
              kv: :boolean,
              streams_shards: :integer,
              streams_flush_interval: :string,
              long_poll_timeout: :integer,
              sse_timeout: :integer,
              pid_file: :string,
              placement: :string,
              lease_ttl: :integer,
              static_nodes: :string,
              peers: :string,
              kv_shards: :integer,
              kv_flush_interval: :string,
              kv_partition_writers: :integer
            ] ++
              for({_key, switch} <- @stream_child_switches, do: {switch, :integer})

  @impl true
  def run(args) do
    {opts, positional} = OptionParser.parse!(args, strict: @switches)
    if opts[:help], do: Help.run(["slap.server"]), else: run_server(opts, positional)
  end

  defp run_server(opts, positional) do
    if positional != [], do: Mix.raise("unexpected arguments: #{Enum.join(positional, " ")}")
    validate_services!(opts)
    validate_placement!(opts)
    validate_port!(opts)
    store = store(opts[:store])
    start_applications(Mix.Project.get())
    SlateDB.set_log_level(:warning)

    config =
      [
        store: store,
        port: Keyword.get(opts, :port, 4437),
        ip: ip(Keyword.get(opts, :ip, "127.0.0.1"))
      ] ++
        strategy(Keyword.get(opts, :placement, "local"), opts) ++
        if(opts[:peers], do: [peers: nodes(opts[:peers])], else: []) ++
        streams(opts) ++
        kv(opts)

    case DynamicSupervisor.start_child(Slap.ServerSupervisor, {Slap.Server, config}) do
      {:ok, _supervisor} -> :ok
      {:error, reason} -> Mix.raise("server failed to start: #{start_error(reason)}")
    end

    if file = opts[:pid_file], do: File.write!(file, System.pid())

    host = url_host(Keyword.get(opts, :ip, "127.0.0.1"))

    if config[:streams],
      do: Mix.shell().info("Durable Streams on http://#{host}:#{config[:port]}/v1/stream/")

    if config[:kv], do: Mix.shell().info("KV on http://#{host}:#{config[:port]}/v1/kv/")

    if !IEx.started?(), do: Process.sleep(:infinity)
  end

  # Mix.install has no project and has started the applications.
  defp start_applications(nil), do: :ok
  defp start_applications(_project), do: Mix.Task.run("app.start")

  defp start_error({:shutdown, {:failed_to_start_child, _id, reason}}),
    do: start_error(reason)

  defp start_error({:EXIT, reason}), do: start_error(reason)

  defp start_error({exception, _stacktrace}) when is_exception(exception),
    do: exception |> Exception.message() |> String.replace(~r/\s+/, " ")

  defp start_error(reason), do: inspect(reason)

  defp validate_placement!(opts) do
    placement = Keyword.get(opts, :placement, "local")

    if Keyword.has_key?(opts, :peers) and placement == "local",
      do: Mix.raise("--peers requires a placement other than local")

    validate_object_lease_store!(placement, opts[:store])

    if Keyword.has_key?(opts, :lease_ttl) and placement != "object-lease",
      do: Mix.raise("--lease-ttl requires --placement object-lease")

    if Keyword.has_key?(opts, :static_nodes) and placement != "static",
      do: Mix.raise("--static-nodes requires --placement static")
  end

  defp validate_object_lease_store!("object-lease", "s3:" <> _url), do: :ok

  defp validate_object_lease_store!("object-lease", _store),
    do: Mix.raise("--placement object-lease requires --store s3:URL")

  defp validate_object_lease_store!(_placement, _store), do: :ok

  defp validate_port!(opts) do
    port = Keyword.get(opts, :port, 4437)

    unless port in 1..65_535,
      do: Mix.raise("--port: expected an integer from 1 to 65535")
  end

  defp validate_services!(opts) do
    if !opts[:streams] and !opts[:kv],
      do: Mix.raise("select --streams, --kv, or both")

    validate_streams!(opts)
    validate_kv!(opts)
  end

  defp validate_streams!(opts) do
    if !opts[:streams] and
         Enum.any?(
           [:streams_shards, :streams_flush_interval, :long_poll_timeout, :sse_timeout] ++
             Keyword.values(@stream_child_switches),
           &Keyword.has_key?(opts, &1)
         ),
       do: Mix.raise("Streams options require --streams")

    if opts[:streams] && Keyword.get(opts, :streams_shards, 8) < 1,
      do: Mix.raise("--streams-shards: expected a positive integer")
  end

  defp validate_kv!(opts) do
    if !opts[:kv] and
         Enum.any?(
           [:kv_shards, :kv_flush_interval, :kv_partition_writers],
           &Keyword.has_key?(opts, &1)
         ),
       do: Mix.raise("KV options require --kv")

    if opts[:kv] && Keyword.get(opts, :kv_shards, 8) < 1,
      do: Mix.raise("--kv-shards: expected a positive integer")

    if opts[:kv] && Keyword.get(opts, :kv_partition_writers, 16) < 1,
      do: Mix.raise("--kv-partition-writers: expected a positive integer")
  end

  defp streams(opts) do
    if opts[:streams] do
      [
        streams: [
          shards: Keyword.get(opts, :streams_shards, 8),
          settings: %{flush_interval: Keyword.get(opts, :streams_flush_interval, "10ms")},
          child_options:
            for(
              {key, switch} <- @stream_child_switches,
              value = opts[switch],
              value != nil,
              do: {key, value}
            ),
          http: Keyword.take(opts, [:long_poll_timeout, :sse_timeout])
        ]
      ]
    else
      []
    end
  end

  defp kv(opts) do
    if opts[:kv],
      do: [
        kv: [
          shards: Keyword.get(opts, :kv_shards, 8),
          settings: %{flush_interval: Keyword.get(opts, :kv_flush_interval, "10ms")},
          child_options:
            if(opts[:kv_partition_writers],
              do: [partition_writers: opts[:kv_partition_writers]],
              else: []
            )
        ]
      ],
      else: []
  end

  defp strategy("local", _opts), do: []

  defp strategy("object-lease", opts),
    do: [strategy: {Strategy.ObjectLease, Keyword.take(opts, [:lease_ttl])}]

  defp strategy("distributed", _opts), do: [strategy: {Strategy.Distributed, []}]

  defp strategy("static", opts) do
    case nodes(opts[:static_nodes]) do
      [_ | _] = nodes -> [strategy: {Strategy.Static, nodes: nodes}]
      _ -> Mix.raise("--placement static needs --static-nodes")
    end
  end

  defp strategy(other, _opts),
    do:
      Mix.raise("--placement: expected local, object-lease, distributed or static, got #{other}")

  defp nodes(nil), do: nil
  defp nodes(list), do: list |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)

  defp store("memory"), do: :memory
  defp store("local:" <> dir), do: {:local, dir}
  defp store("s3:" <> url), do: {:url, url}
  defp store(nil), do: Mix.raise("--store is required; use memory for ephemeral data")
  defp store(other), do: Mix.raise("--store: expected memory, local:DIR or s3:URL, got #{other}")

  defp ip(value) do
    case :inet.parse_address(String.to_charlist(value)) do
      {:ok, address} -> address
      {:error, _} -> Mix.raise("--ip: expected an IP address, got #{value}")
    end
  end

  defp url_host(ip) do
    if String.contains?(ip, ":"), do: "[#{ip}]", else: ip
  end
end
