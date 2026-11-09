defmodule Slap.Inspect.Operation do
  @moduledoc false

  @messages [:sid, :from, :until]
  @read_limits [:max_key_bytes, :max_value_bytes, :target_bytes]
  @page [:limit, :cursor, :cursor_exclusive | @read_limits]

  # Definition order breaks ties between equally matching command forms.
  @forms [
    home: %{
      scope: :every_cluster,
      command: "home",
      view: :home,
      options: [],
      page: [],
      listing: true,
      required: [],
      route: :entry,
      budget: 10_000
    },
    raw: %{
      command: "scan",
      view: :raw,
      options: [:prefix],
      listing: true,
      required: [],
      target: {Slap.Cluster.Inspection, :scan},
      menu: 1
    },
    get: %{
      command: "get",
      view: :get,
      options: [:key, :value_range],
      page: [:max_value_bytes],
      required: [:key],
      target: {Slap.Cluster.Inspection, :get}
    },
    streams: %{
      command: "streams",
      view: :streams,
      options: [:prefix],
      listing: true,
      required: [],
      target: {Slap.Streams.Introspect, :list},
      menu: 2
    },
    stream: %{
      command: "stream",
      view: :stream,
      options: [:path, :placement_key],
      page: [:max_value_bytes],
      required: [:path],
      target: {Slap.Streams.Introspect, :stream}
    },
    messages: %{
      tail: true,
      command: "messages",
      view: :messages,
      options: @messages,
      listing: true,
      required: [:sid],
      target: {Slap.Streams.Introspect, :messages}
    },
    logical: %{
      tail: true,
      command: "logical",
      view: :logical,
      options: [:path, :placement_key, :expected_sid, :from, :until, :retained],
      listing: true,
      required: [:path],
      target: {Slap.Streams.Introspect, :messages}
    },
    retention: %{
      command: "retention",
      view: :retention,
      options: [:placement_key, :sid, :pins],
      listing: true,
      required: [:sid],
      target: {Slap.Streams.Introspect, :retention}
    },
    retention_metadata: %{
      command: "retention",
      view: :retention,
      options: [:path, :placement_key, :sid, :pins],
      listing: true,
      required: [:sid, :path],
      target: {Slap.Streams.Introspect, :retention}
    },
    kv_partitions: %{
      command: "kv",
      view: :kv,
      options: [],
      page: [:limit, :cursor, :max_key_bytes, :target_bytes],
      listing: true,
      required: [],
      target: {Slap.KV.Introspect, :list_partitions},
      route: :partition,
      menu: 3
    },
    kv: %{
      command: "kv",
      view: :kv,
      options: [:partition, :key_prefix],
      listing: true,
      required: [:partition],
      target: {Slap.KV.Introspect, :scan},
      route: :partition
    },
    files_partitions: %{
      command: "files",
      view: :files,
      options: [:instance],
      page: [:limit, :cursor, :max_key_bytes, :target_bytes],
      listing: true,
      required: [],
      target: {Slap.Files.Introspect, :partitions},
      route: :partition,
      menu: 4
    },
    files: %{
      command: "files",
      view: :files,
      options: [:partition, :key_prefix, :instance],
      listing: true,
      required: [:partition],
      target: {Slap.Files.Introspect, :scan},
      route: :partition
    },
    object: %{
      command: "object",
      view: :object,
      options: [:object_key, :instance, :value_range],
      page: [:max_value_bytes],
      required: [:object_key],
      target: {Slap.Files.Introspect, :object},
      route: :entry
    },
    log_content: %{
      manual: true,
      command: "log",
      flags: [:content],
      view: :log_content,
      options: [:base, :placement_key, :preflight, :at_offset, :max_input_bytes, :max_entries],
      page: [],
      required: [:base],
      budget: 10_000
    },
    log_component: %{
      command: "log",
      view: :log,
      options: [:base, :component, :placement_key, :expected_sid, :from, :until],
      listing: true,
      required: [:base, :component],
      target: {Slap.SnapshotLog.Introspect, :messages}
    },
    log_catalog: %{
      command: "log",
      view: :log,
      options: [:base, :placement_key],
      required: [:base],
      target: {Slap.SnapshotLog.Introspect, :scan}
    },
    log_list: %{
      command: "log",
      view: :log,
      options: [],
      listing: true,
      required: [],
      target: {Slap.SnapshotLog.Introspect, :list},
      menu: 5
    },
    yjs_content: %{
      manual: true,
      command: "yjs",
      flags: [:content],
      view: :yjs_content,
      options: [
        :base,
        :placement_key,
        :preflight,
        :roots,
        :at_offset,
        :max_input_bytes,
        :max_entries,
        :max_output_bytes
      ],
      page: [],
      required: [:base],
      budget: 10_000
    },
    yjs_log: %{
      command: "yjs",
      view: :yjs,
      options: [:base, :placement_key, :from, :until],
      required: [:base],
      target: {Slap.Yjs.Introspect, :messages}
    },
    yjs_messages: %{
      command: "yjs",
      view: :yjs,
      options: [:placement_key | @messages],
      listing: true,
      required: [:sid],
      target: {Slap.Yjs.Introspect, :messages}
    },
    yjs_list: %{
      command: "yjs",
      view: :yjs,
      options: [],
      listing: true,
      required: [],
      target: {Slap.Yjs.Introspect, :list},
      menu: 6
    }
  ]

  @definitions Map.new(@forms)
  # :any views read any cluster. The other names must match the services/0 of
  # Slap.Streams.ShardChildren and Slap.KV.PartitionWriter.
  @services %{
    home: :any,
    raw: :any,
    get: :any,
    streams: :streams,
    stream: :streams,
    messages: :streams,
    logical: :streams,
    retention: :streams,
    log: :streams,
    log_content: :streams,
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

  @content_views for {_operation, %{flags: flags} = form} <- @definitions,
                     :content in flags,
                     into: %{},
                     do: {form.command, form.view}

  @commands @definitions
            |> Enum.reject(fn {_operation, definition} -> Map.has_key?(definition, :flags) end)
            |> Map.new(fn {_operation, definition} -> {definition.command, definition.view} end)

  def read_limits, do: @read_limits

  def page(%{tail: true} = form), do: [:before | Map.get(form, :page, @page)]
  def page(form), do: Map.get(form, :page, @page)

  def forms(command), do: for({_operation, %{command: ^command} = form} <- @forms, do: form)

  def commands, do: Map.keys(@commands)

  def command([]), do: {:ok, :raw}

  def command([name]) do
    case Map.fetch(@commands, name) do
      {:ok, view} -> {:ok, view}
      :error -> {:error, :unknown_command}
    end
  end

  def command(_commands), do: {:error, :one_command_required}

  @doc """
  Chooses a command form by its flags and required options. Ranks forms by
  how many of these options are supplied, then how few are missing.
  Definition order breaks remaining ties.
  """
  def normalize(view, opts) do
    case for(
           {operation, form} <- @forms,
           view in [form.view | content_of(form)],
           do: {operation, form}
         ) do
      [] ->
        {:error, :unknown_view}

      forms ->
        {operation, form} =
          Enum.min_by(forms, fn {_operation, form} ->
            {present, missing} = Enum.split_with(selectors(form), &opts[&1])
            {-length(present), length(missing)}
          end)

        {:ok, operation, form}
    end
  end

  defp content_of(%{flags: flags, command: command}) do
    if :content in flags,
      do: for(%{command: ^command, view: view} <- Map.values(@definitions), do: view),
      else: []
  end

  defp content_of(_form), do: []

  def selectors(form), do: Map.get(form, :flags, []) ++ form.required

  def manual?(operation), do: field(operation, :manual, false)

  def refresh?(operation), do: not (manual?(operation) or every_cluster?(operation))

  def tail?(operation), do: field(operation, :tail, false)

  def content_view(operation), do: @content_views[Map.fetch!(@definitions, operation).command]

  def every_cluster?(operation), do: field(operation, :scope, :cluster) == :every_cluster

  defp field(operation, name, default),
    do: Map.get(Map.fetch!(@definitions, operation), name, default)

  def target(operation), do: Map.fetch!(@definitions, operation).target

  def route(operation), do: field(operation, :route, :stream)

  @doc """
  Checks whether page limits only affect how many rows are returned.
  Snapshot log overviews (`log --base`) and `yjs --base` need complete
  component pages to identify the log, so they are excluded.
  """
  def listing?(operation), do: field(operation, :listing, false)

  @doc """
  Checks whether a listing allows shard selection without a path, base or partition.
  """
  def spans_shard?(operation) do
    definition = Map.fetch!(@definitions, operation)

    Map.get(definition, :listing, false) and definition.required == [] and
      route(operation) != :entry
  end

  def budget(operation), do: field(operation, :budget, 5000)
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
