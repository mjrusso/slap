defmodule Slap.Inspect.CLI do
  @moduledoc false

  alias Slap.Inspect.{Command, Feedback, JSON, Operation, Query, Remote, Request, Requests}

  @usage """
  Usage: slap-inspect --node NODE --cookie-file PATH --cluster MODULE [options] [command]

  Opens a read-only terminal inspector for a running Slap cluster.
  Add --json to write one JSON object and exit.

      slap-inspect --node slap@host --cookie-file ~/.slap-cookie --cluster Slap.Streams.Cluster
      slap-inspect --node slap@host --cookie-file ~/.slap-cookie --cluster Slap.KV.Cluster --json kv --partition users

  Commands: scan, get, streams, stream, messages, logical, retention, kv,
  files, object, log, yjs. Use --shard N for raw scans or sid lookups,
  --path PATH for a stream, --base PATH for a log, --partition NAME for KV,
  and --sid N for messages or retention. --placement-key overrides stream
  routing. --prefix, --key, --key-prefix and --cursor take hexadecimal pairs.
  Use --offset N --length N for a value or object byte range. --longnames
  selects long node names. --budget MS defaults to 5000, or 10000 for
  document content (maximum 30000).
  --refresh MS refreshes terminal pages at that interval, except Yjs content.

  Use yjs --content --base PATH --root text:body to read document content.
  Repeat --root TYPE:NAME for text, array, map or xml_fragment roots.
  In the terminal, i opens the root interpretation chooser for loaded content.
  Add --at-offset N to replay retained updates before N without a snapshot.
  --max-input-bytes, --max-entries and --max-output-bytes set the input,
  entry count and JSON output limits.
  Input sizes count wire bytes: each message's body plus a four-byte header.

  Content buffers use {"$bytes": "<hex>"}. User map keys starting with $
  gain another $ at every nesting level: "$bytes" becomes "$$bytes".
  Root names are unchanged. A single leading $ is reserved for encoder tags.

  JSON preserves document content; raw binaries are hexadecimal strings.
  Errors exit nonzero.
  Reads can include durable writes whose replies are pending or lost.
  Distributed Erlang permits full remote access; restrict it as you would
  a remote console. The inspector never opens a database.
  """

  @connection_options [:node, :cookie_file, :cluster, :json, :longnames, :refresh]
  @switches [
    node: :string,
    cookie_file: :string,
    cluster: :string,
    json: :boolean,
    longnames: :boolean,
    shard: :integer,
    prefix: :string,
    key: :string,
    key_prefix: :string,
    partition: :string,
    path: :string,
    base: :string,
    component: :string,
    sid: :integer,
    expected_sid: :integer,
    from: :integer,
    until: :integer,
    limit: :integer,
    cursor: :string,
    cursor_exclusive: :boolean,
    offset: :integer,
    length: :integer,
    placement_key: :string,
    object_key: :string,
    instance: :string,
    budget: :integer,
    refresh: :integer,
    max_value_bytes: :integer,
    max_key_bytes: :integer,
    target_bytes: :integer,
    pins: :boolean,
    content: :boolean,
    root: :keep,
    at_offset: :integer,
    max_input_bytes: :integer,
    max_entries: :integer,
    max_output_bytes: :integer
  ]

  def main([arg]) when arg in ["--help", "-h"], do: IO.puts(@usage)

  def main(args) do
    case run(args) do
      :ok ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, Elixir.JSON.encode!(%{error: error_term(reason)}))
        exit({:shutdown, 1})
    end
  end

  # Error reasons hold option names and messages, so their binaries stay
  # text. JSON.encode/1 writes binaries as hexadecimal, for stored bytes.
  defp error_term(reason) when is_binary(reason),
    do: if(String.printable?(reason), do: reason, else: inspect(reason))

  defp error_term(reason) when is_tuple(reason),
    do: reason |> Tuple.to_list() |> error_term()

  defp error_term(reason) when is_list(reason), do: Enum.map(reason, &error_term/1)

  defp error_term(reason) when is_map(reason),
    do: Map.new(reason, fn {key, value} -> {error_key(key), error_term(value)} end)

  defp error_term(reason) when is_atom(reason) or is_number(reason), do: reason
  defp error_term(reason), do: inspect(reason)

  defp error_key(key) when is_atom(key), do: key
  defp error_key(key), do: inspect(key)

  def parse(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, commands, []} -> connection(opts, commands)
      {_opts, _commands, invalid} -> {:error, {:invalid_options, Enum.map(invalid, &elem(&1, 0))}}
    end
  end

  defp connection(opts, commands) do
    with {:ok, entry} <- required(opts, :node),
         {:ok, cookie_file} <- required(opts, :cookie_file),
         {:ok, cluster} <- required(opts, :cluster),
         :ok <- Request.refresh(opts[:refresh] || 0),
         {:ok, request} <- query(opts, commands) do
      {:ok,
       %{
         entry: String.to_atom(entry),
         cluster: Module.concat([cluster]),
         cookie_file: cookie_file,
         name_domain: if(opts[:longnames], do: :longnames, else: :shortnames)
       }, request, %{json: opts[:json] || false, refresh: opts[:refresh] || 0}}
    end
  end

  def parse_query(args) do
    case OptionParser.parse(args, strict: Keyword.drop(@switches, @connection_options)) do
      {opts, commands, []} ->
        query(opts, commands)

      {_opts, _commands, invalid} ->
        {:error, {:invalid_query_options, Enum.map(invalid, &elem(&1, 0))}}
    end
  end

  defp query(opts, commands) do
    with {:ok, view} <- Operation.command(commands),
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

  def command(request) do
    {:ok, operation, definition} = Operation.normalize(request.view, request.opts)

    command =
      Enum.join(
        [definition.command | Enum.map(Map.get(definition, :flags, []), &Feedback.option/1)],
        " "
      )

    options = Enum.flat_map(request.opts, &command_option/1)
    shard = if is_nil(request.shard), do: [], else: ["--shard", to_string(request.shard)]

    budget =
      if request.budget == Operation.budget(operation),
        do: [],
        else: ["--budget", to_string(request.budget)]

    Enum.join([command | shard ++ options ++ budget], " ")
  end

  defp command_option({:roots, roots}),
    do: Enum.flat_map(roots, fn {name, type} -> ["--root", Command.quote("#{type}:#{name}")] end)

  defp command_option({:value_range, {offset, length}}),
    do: ["--offset", to_string(offset), "--length", to_string(length)]

  defp command_option({key, value}) when key in [:prefix, :key_prefix, :key, :cursor],
    do: [Feedback.option(key), Command.quote(Base.encode16(value, case: :lower))]

  defp command_option({_key, false}), do: []
  defp command_option({key, true}), do: [Feedback.option(key)]

  defp command_option({key, value}) do
    if Keyword.has_key?(@switches, key),
      do: [Feedback.option(key), Command.quote(to_string(value))],
      else: []
  end

  def run(args) do
    with {:ok, connection, request, mode} <- parse(args),
         :ok <-
           Remote.connect(connection.entry, connection.cookie_file,
             name_domain: connection.name_domain
           ) do
      if mode.json, do: json(connection, request), else: terminal(connection, request, mode)
    end
  end

  defp json(connection, request) do
    {:ok, worker} = Requests.start_link(client: self(), read: &Query.execute(connection, &1))
    id = make_ref()
    Requests.submit(worker, id, request)
    await_json(id)
  end

  defp await_json(id) do
    receive do
      {:inspection_result, ^id, {:ok, page}} ->
        IO.puts(JSON.encode(page))
        :ok

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

  defp terminal(connection, request, mode),
    do:
      Breeze.Server.run(
        view: Slap.Inspect.View,
        start_opts: [connection: connection, request: request, refresh: mode.refresh]
      )
end
