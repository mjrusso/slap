defmodule Slap.Inspect.Operation do
  @moduledoc false

  @stream [:path, :placement_key, :expected_sid]
  @messages [:sid, :from, :until]
  @content [:roots, :at_offset, :max_input_bytes, :max_entries, :max_output_bytes]
  @log [:base, :component, :placement_key, :expected_sid, :from, :until] ++ @content
  @yjs [:base, :placement_key, :expected_sid] ++ @messages ++ @content

  @definitions %{
    raw: %{
      command: "scan",
      view: :raw,
      options: [:prefix],
      required: [],
      target: {Slap.Cluster.Inspection, :scan},
      menu: 1
    },
    get: %{
      command: "get",
      view: :get,
      options: [:key, :value_range],
      required: [:key],
      target: {Slap.Cluster.Inspection, :get}
    },
    streams: %{
      command: "streams",
      view: :streams,
      options: [:prefix],
      required: [],
      target: {Slap.Streams.Introspect, :list},
      menu: 2
    },
    stream: %{
      command: "stream",
      view: :stream,
      options: @stream,
      required: [:path],
      target: {Slap.Streams.Introspect, :stream}
    },
    messages: %{
      command: "messages",
      view: :messages,
      options: @messages,
      required: [:sid],
      target: {Slap.Streams.Introspect, :messages}
    },
    logical: %{
      command: "logical",
      view: :logical,
      options: @stream ++ [:from, :until, :retained],
      required: [:path],
      target: {Slap.Streams.Introspect, :messages}
    },
    retention: %{
      command: "retention",
      view: :retention,
      options: @stream ++ [:sid, :pins],
      required: [:sid],
      target: {Slap.Streams.Introspect, :retention}
    },
    retention_metadata: %{
      command: "retention",
      view: :retention,
      options: @stream ++ [:sid, :pins],
      required: [:sid, :path],
      target: {Slap.Streams.Introspect, :retention}
    },
    kv: %{
      command: "kv",
      view: :kv,
      options: [:partition, :key_prefix],
      required: [],
      target: {Slap.KV.Introspect, :scan},
      route: :partition,
      menu: 3
    },
    files: %{
      command: "files",
      view: :files,
      options: [:partition, :key_prefix, :instance],
      required: [],
      target: {Slap.Files.Introspect, :scan},
      route: :partition,
      menu: 4
    },
    object: %{
      command: "object",
      view: :object,
      options: [:object_key, :instance, :value_range],
      required: [:object_key],
      target: {Slap.Files.Introspect, :object},
      route: :entry
    },
    log_catalog: %{
      command: "log",
      view: :log,
      options: @log,
      required: [:base],
      target: {Slap.SnapshotLog.Introspect, :scan},
      menu: 5
    },
    log_component: %{
      command: "log",
      view: :log,
      options: @log,
      required: [:base, :component],
      target: {Slap.SnapshotLog.Introspect, :messages}
    },
    yjs_messages: %{
      command: "yjs",
      view: :yjs,
      options: @yjs,
      required: [:sid],
      target: {Slap.Yjs.Introspect, :messages}
    },
    yjs_log: %{
      command: "yjs",
      view: :yjs,
      options: @yjs,
      required: [:base],
      target: {Slap.Yjs.Introspect, :messages},
      menu: 6
    },
    yjs_content: %{
      command: "yjs",
      flags: [:content],
      view: :yjs_content,
      options: [:base, :placement_key, :preflight] ++ @content,
      required: [:base],
      budget: 10_000
    }
  }
  # :any views read any cluster. The other names must match the services/0 of
  # Slap.Streams.ShardChildren and Slap.KV.PartitionWriter.
  @services %{
    raw: :any,
    get: :any,
    streams: :streams,
    stream: :streams,
    messages: :streams,
    logical: :streams,
    retention: :streams,
    log: :streams,
    yjs: :streams,
    yjs_content: :streams,
    kv: :kv,
    files: :kv,
    object: :kv
  }

  views = MapSet.new(Map.values(@definitions), & &1.view)
  named = MapSet.new(Map.keys(@services))

  unless MapSet.equal?(views, named),
    do:
      raise(
        "@services must name exactly the views of @definitions; " <>
          "missing: #{inspect(MapSet.to_list(MapSet.difference(views, named)))}, " <>
          "extra: #{inspect(MapSet.to_list(MapSet.difference(named, views)))}"
      )

  @commands @definitions
            |> Enum.reject(fn {_operation, definition} -> Map.has_key?(definition, :flags) end)
            |> Map.new(fn {_operation, definition} -> {definition.command, definition.view} end)

  def command([]), do: {:ok, :raw}

  def command([name]) do
    case Map.fetch(@commands, name) do
      {:ok, view} -> {:ok, view}
      :error -> {:error, :unknown_command}
    end
  end

  def command(_commands), do: {:error, :one_command_required}

  def normalize(:yjs, opts) do
    operation =
      cond do
        opts[:content] -> :yjs_content
        opts[:base] -> :yjs_log
        true -> :yjs_messages
      end

    definition(operation)
  end

  def normalize(:log, opts),
    do: definition(if(opts[:component], do: :log_component, else: :log_catalog))

  def normalize(:retention, opts),
    do: definition(if(opts[:path], do: :retention_metadata, else: :retention))

  def normalize(view, _opts), do: definition(view)

  defp definition(operation) do
    case Map.fetch(@definitions, operation) do
      {:ok, definition} -> {:ok, operation, definition}
      :error -> {:error, :unknown_view}
    end
  end

  def options(view) do
    @definitions
    |> Map.values()
    |> Enum.filter(&(&1.view == view))
    |> Enum.flat_map(& &1.options)
    |> Enum.uniq()
  end

  @doc "The option sets that satisfy `view`, with the menu definition's set first."
  def required(view) do
    sets =
      @definitions
      |> Map.values()
      |> Enum.filter(&(&1.view == view))
      |> Enum.sort_by(&is_nil(&1[:menu]))
      |> Enum.map(& &1.required)
      |> Enum.uniq()

    # A set that contains another set only adds options to it.
    Enum.reject(sets, fn set -> Enum.any?(sets, &(&1 != set and &1 -- set == [])) end)
  end

  def target(operation), do: Map.fetch!(@definitions, operation).target

  def route(operation), do: Map.get(Map.fetch!(@definitions, operation), :route, :stream)
  def budget(operation), do: Map.get(Map.fetch!(@definitions, operation), :budget, 5000)
  def targets, do: for(%{target: target} <- Map.values(@definitions), uniq: true, do: target)

  def service(view), do: Map.fetch!(@services, view)

  def views do
    @definitions
    |> Map.values()
    |> Enum.filter(&Map.has_key?(&1, :menu))
    |> Enum.sort_by(& &1.menu)
    |> Enum.map(& &1.view)
  end
end
