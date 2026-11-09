defmodule Slap.Inspect.CLI do
  @moduledoc false

  alias Slap.Inspect.{
    Clusters,
    Command,
    JSON,
    Operation,
    Options,
    Query,
    Remote,
    Request,
    Requests
  }

  @usage """
  Usage: slap-inspect --node NODE [--cookie-file PATH] [--cluster MODULE] [options] [command]

  Opens a read-only terminal inspector for the Slap clusters running on NODE.
  Add --json to write one JSON object and exit.

      slap-inspect --node slap@host
      slap-inspect --node slap@host --cookie-file ~/.slap-cookie --json kv --partition users
      slap-inspect --node slap@host --cookie-file ~/.slap-cookie --cluster MyApp.Cluster scan

  Without --cluster, the inspector lists the clusters running on NODE and
  shows each one's views; keys 1-9 open them. Given no --cluster, command or
  query option, the terminal opens home: every cluster's views, with row
  counts per shard, up to 50 per view and shard. h returns to it. A command
  supplied at startup opens the first listed cluster that serves it.

  With --json, commands other than home and clusters require exactly one
  cluster to serve them; when several do, choose one with --cluster.
  home --json prints the home view. The clusters command prints the cluster
  list as JSON, with or without --json.

  --cluster reads one cluster and also works with servers that cannot list
  their clusters.

  Commands: clusters, home, scan, get, streams, stream, messages, logical,
  retention, kv, files, object, log, yjs. Use --shard N for raw scans or sid
  lookups, --path PATH for a stream, --base PATH for a log or Yjs document
  and --partition NAME for KV and files. Use --sid N for messages or retention.
  Without --base, log lists logs and yjs lists documents. Without --partition,
  kv and files list partitions. --placement-key overrides stream routing.
  Commands reject options they do not use.
  --prefix, --key, --key-prefix and --cursor take hexadecimal pairs.
  Use --offset N --length N for a value or object byte range.

  A listing is read one page at a time. A page that has more includes a
  hexadecimal cursor; with --json, the command for the next page is written
  to stderr. Pass --cursor HEX back, with --cursor-exclusive if the page
  reports cursor_exclusive: true. --limit N sets the rows per page (at most
  200), --target-bytes N the bytes per page (at most 1 MiB),
  --max-value-bytes N the bytes kept from each value (at most 512 KiB) and
  --max-key-bytes N from each key (at most 4096). Terminal pages default to
  the maximums. --json listings default to 50 rows and 256 KiB. Single
  records, log --base without --component, and yjs --base keep the
  maximums. get, stream and object take only --max-value-bytes.
  log --content and yjs --content take none of these.
  --budget MS defaults to 5000, or 10000 for home, log content and Yjs content
  (maximum 30000).
  --refresh MS refreshes terminal pages at that interval, skipping home,
  log content and Yjs content.

  Without --cookie-file, the inspector uses Erlang's default cookie
  (~/.erlang.cookie). It uses long node names when NODE's host contains a
  dot, as in slap@127.0.0.1; --longnames selects them for other hosts.

  Use log --content --base PATH to read a log's snapshot and later entries.
  Use yjs --content --base PATH --root text:body to read document content.
  Repeat --root TYPE:NAME for text, array, map or xml_fragment roots.
  In the terminal, i opens the root interpretation chooser for loaded content.
  Add --at-offset N to replay retained updates before N without a snapshot.
  --max-input-bytes and --max-entries limit the input size and entry count.
  Yjs content also accepts --max-output-bytes to limit the JSON output size.
  Input sizes count wire bytes: each message's body plus a four-byte header.

  Content buffers use {"$bytes": "<hex>"}. User map keys starting with $
  gain another $ at every nesting level: "$bytes" becomes "$$bytes".
  Root names are unchanged. A single leading $ is reserved for encoder tags.

  JSON writes printable text as strings and other binaries as
  {"$bytes": "<hex>"}. Page cursors remain hexadecimal for --cursor.
  Errors exit nonzero.
  Reads can include durable writes whose replies are pending or lost.
  Distributed Erlang permits full remote access; restrict it as you would
  a remote console. The inspector never opens a database.
  """

  @json_page [limit: 50, target_bytes: 256 * 1024]

  def main([arg]) when arg in ["--help", "-h"], do: IO.puts(@usage)

  def main(args) do
    case run(args) do
      :ok ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, JSON.encode(%{error: reason}))
        exit({:shutdown, 1})
    end
  end

  def parse(args) do
    case OptionParser.parse(args, strict: Options.switches()) do
      {opts, commands, []} ->
        with {:ok, endpoint} <- endpoint(opts), do: selection(endpoint, opts, commands)

      {_opts, _commands, invalid} ->
        {:error, {:invalid_options, Enum.map(invalid, &elem(&1, 0))}}
    end
  end

  defp endpoint(opts) do
    with {:ok, entry} <- required(opts, :node),
         {:ok, host} <- host(entry) do
      {:ok,
       %{
         entry: String.to_atom(entry),
         cookie_file: opts[:cookie_file],
         name_domain: name_domain(host, opts)
       }}
    end
  end

  # The inverse of endpoint/1.
  defp endpoint_args(endpoint) do
    cookie = if endpoint.cookie_file, do: ["--cookie-file", endpoint.cookie_file], else: []
    longnames = if endpoint.name_domain == :longnames, do: ["--longnames"], else: []
    ["--node", Atom.to_string(endpoint.entry)] ++ cookie ++ longnames
  end

  defp host(entry) do
    case String.split(entry, "@", parts: 2) do
      [_name, host] -> {:ok, host}
      _ -> {:error, {:invalid, "--node must be NAME@HOST"}}
    end
  end

  # Short names cannot have a dot in the host, so a dot requires long names.
  # Long names without one (slap@localhost) need --longnames.
  defp name_domain(host, opts) do
    if opts[:longnames] || String.contains?(host, "."), do: :longnames, else: :shortnames
  end

  defp selection(endpoint, opts, ["clusters"]) do
    if opts[:cluster],
      do: {:error, {:invalid, "clusters lists every cluster; omit --cluster"}},
      else: {:ok, endpoint, :clusters}
  end

  defp selection(endpoint, opts, []) do
    if Options.query(opts) == [] and is_nil(opts[:cluster]) and opts[:json] != true,
      do: query_selection(endpoint, opts, ["home"]),
      else: query_selection(endpoint, opts, [])
  end

  defp selection(endpoint, opts, commands), do: query_selection(endpoint, opts, commands)

  defp query_selection(endpoint, opts, commands) do
    with {:ok, cluster} <- cluster(opts),
         :ok <- Request.refresh(opts[:refresh] || 0),
         {:ok, request} <- query(opts, commands),
         :ok <- one_cluster(request, opts) do
      json = opts[:json] || false

      {:ok, endpoint, cluster, json_page(request, json),
       %{json: json, refresh: opts[:refresh] || 0}}
    end
  end

  defp one_cluster(request, opts) do
    if Operation.every_cluster?(request.operation) and opts[:cluster],
      do: {:error, {:invalid, "#{request.view} lists every cluster; omit --cluster"}},
      else: :ok
  end

  defp json_page(request, false), do: request

  defp json_page(request, true) do
    if Operation.listing?(request.operation),
      do: %{request | opts: Keyword.merge(@json_page, request.opts)},
      else: request
  end

  defp cluster(opts) do
    case opts[:cluster] do
      nil -> {:ok, nil}
      "" -> {:error, {:required, :cluster}}
      name -> {:ok, Module.concat([name])}
    end
  end

  def parse_query(args) do
    case OptionParser.parse(args, strict: Options.query_switches()) do
      {opts, commands, []} ->
        query(opts, commands)

      {_opts, _commands, invalid} ->
        {:error, {:invalid_query_options, Enum.map(invalid, &elem(&1, 0))}}
    end
  end

  defp query(opts, commands) do
    with {:ok, view} <- Operation.command(commands),
         {:ok, _operation, form} <- Operation.normalize(view, opts),
         :ok <- Options.check(opts, List.first(commands, "scan"), form),
         {:ok, read_opts} <- read_opts(opts, view),
         do:
           Request.new(%{view: view, shard: opts[:shard], budget: opts[:budget], opts: read_opts})
  end

  defp required(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:required, key}}
    end
  end

  defp read_opts(opts, view) do
    with {:ok, parsed} <-
           Enum.reduce_while([:prefix, :key_prefix, :key, :cursor], {:ok, opts}, &hex_option/2),
         {:ok, parsed} <- component_option(parsed),
         {:ok, parsed} <- roots(parsed) do
      {:ok, range(parsed, opts, view)}
    end
  end

  defp roots(opts) do
    Enum.reduce_while(Keyword.get_values(opts, :root), {:ok, []}, fn value, {:ok, roots} ->
      case root(value) do
        {:ok, root} -> {:cont, {:ok, [root | roots]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, roots} ->
        {:ok, opts |> Keyword.delete(:root) |> Keyword.put(:roots, Enum.reverse(roots))}

      error ->
        error
    end
  end

  defp root(value) do
    types = %{"text" => :text, "array" => :array, "map" => :map, "xml_fragment" => :xml_fragment}

    with [type, name] when name != "" <- String.split(value, ":", parts: 2),
         {:ok, type} <- Map.fetch(types, type) do
      {:ok, {name, type}}
    else
      _ -> {:error, :invalid_root_interpretation}
    end
  end

  defp hex_option(field, {:ok, opts}) do
    case Keyword.fetch(opts, field) do
      {:ok, value} -> decode_option(field, value, opts)
      :error -> {:cont, {:ok, opts}}
    end
  end

  defp decode_option(field, value, opts) do
    case hex(value) do
      {:ok, bytes} -> {:cont, {:ok, Keyword.put(opts, field, bytes)}}
      :error -> {:halt, {:error, {:invalid_hex, field}}}
    end
  end

  defp component_option(opts) do
    case Keyword.get(opts, :component) do
      nil -> {:ok, opts}
      "index" -> {:ok, Keyword.put(opts, :component, :index)}
      "control" -> {:ok, Keyword.put(opts, :component, :control)}
      "updates" -> {:ok, Keyword.put(opts, :component, :updates)}
      _ -> {:error, :invalid_component}
    end
  end

  defp range(opts, original, view) do
    opts =
      if opts[:instance], do: Keyword.update!(opts, :instance, &Module.concat([&1])), else: opts

    default_length =
      min(original[:max_value_bytes] || Request.range_bytes(), Request.range_bytes())

    if (view == :object or original[:offset]) || original[:length],
      do:
        Keyword.put(
          opts,
          :value_range,
          {original[:offset] || 0, original[:length] || default_length}
        ),
      else: opts
  end

  defp hex(value) do
    if Regex.match?(~r/\A(?:[0-9a-fA-F]{2} *)*\z/, value),
      do: Base.decode16(String.replace(value, " ", ""), case: :mixed),
      else: :error
  end

  def command(request), do: request |> words() |> Enum.map_join(" ", &Command.quote/1)

  defp words(request) do
    {:ok, operation, definition} = Operation.normalize(request.view, request.opts)
    flags = Enum.map(Map.get(definition, :flags, []), &Options.option/1)
    options = Enum.flat_map(request.opts, &command_option/1)
    shard = if is_nil(request.shard), do: [], else: ["--shard", to_string(request.shard)]

    budget =
      if request.budget == Operation.budget(operation),
        do: [],
        else: ["--budget", to_string(request.budget)]

    [definition.command | flags] ++ shard ++ options ++ budget
  end

  def shell_command(endpoint, cluster, request) do
    ["slap-inspect" | endpoint_args(endpoint)]
    |> Enum.concat(["--cluster", inspect(cluster), "--json" | words(request)])
    |> Enum.map_join(" ", &Command.shell_quote/1)
  end

  defp command_option({:roots, roots}),
    do: Enum.flat_map(roots, fn {name, type} -> ["--root", "#{type}:#{name}"] end)

  defp command_option({:value_range, {offset, length}}),
    do: ["--offset", to_string(offset), "--length", to_string(length)]

  defp command_option({key, value}) when key in [:prefix, :key_prefix, :key, :cursor],
    do: [Options.option(key), Base.encode16(value, case: :lower)]

  defp command_option({_key, false}), do: []
  defp command_option({key, true}), do: [Options.option(key)]

  defp command_option({key, value}) do
    if Options.switch?(key),
      do: [Options.option(key), to_string(value)],
      else: []
  end

  def run(args) do
    case parse(args) do
      {:ok, endpoint, :clusters} ->
        with :ok <- connect(endpoint), do: list(endpoint.entry)

      {:ok, endpoint, cluster, request, mode} ->
        with :ok <- connect(endpoint), do: read(endpoint, cluster, request, mode)

      error ->
        error
    end
  end

  defp connect(endpoint),
    do: Remote.connect(endpoint.entry, endpoint.cookie_file, name_domain: endpoint.name_domain)

  defp read(endpoint, cluster, request, %{json: true}), do: json(endpoint, cluster, request)

  defp read(endpoint, cluster, request, mode),
    do: terminal(endpoint.entry, cluster, request, mode)

  defp list(entry) do
    with {:ok, clusters} <- Clusters.discover(entry) do
      IO.puts(JSON.encode(%{clusters: Enum.map(clusters, &listed/1)}))
    end
  end

  defp listed(cluster),
    do: %{cluster | cluster: inspect(cluster.cluster)} |> Map.put(:views, Clusters.views(cluster))

  defp json(endpoint, cluster, request) do
    with {:ok, clusters} <- Clusters.load(endpoint.entry, cluster),
         {:ok, cluster} <- json_cluster(clusters, request),
         {:ok, page} <- fetch_page(endpoint.entry, cluster, request) do
      IO.puts(JSON.encode(page))

      case Request.next(request, page) do
        nil -> :ok
        next -> IO.puts(:stderr, "Next page: " <> shell_command(endpoint, cluster, next))
      end
    end
  end

  # Home discovers clusters again, even if the connected cluster has stopped.
  defp json_cluster(clusters, request) do
    if Operation.every_cluster?(request.operation),
      do: Clusters.first(clusters, request.view),
      else: Clusters.only(clusters, request.view)
  end

  defp fetch_page(entry, cluster, request) do
    {:ok, worker} = Requests.start_link(client: self(), read: &Query.execute/2)
    id = make_ref()
    Requests.submit(worker, id, %{entry: entry, cluster: cluster}, request)
    await_json(id)
  end

  defp await_json(id) do
    receive do
      {:inspection_result, ^id, {:ok, page}} ->
        {:ok, page}

      {:inspection_result, ^id, {:error, _} = error} ->
        error

      {:inspection_owner, ^id, _owner, _shard} ->
        await_json(id)

      {:inspection_stalled, ^id, owner, shard} ->
        {:error, {:owner_read_stalled, owner, shard}}

      {:inspection_stalled, ^id} ->
        {:error, {:owner_read_stalled, nil}}
    end
  end

  defp terminal(entry, cluster, request, mode) do
    with {:ok, clusters} <- Clusters.load(entry, cluster),
         {:ok, cluster} <- Clusters.first(clusters, request.view) do
      Breeze.Server.run(
        view: Slap.Inspect.View,
        start_opts: [
          connection: %{entry: entry, cluster: cluster},
          clusters: clusters,
          request: request,
          refresh: mode.refresh
        ]
      )
    end
  end
end
