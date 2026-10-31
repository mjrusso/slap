defmodule Slap.SnapshotLog.Introspect do
  @moduledoc false

  alias Slap.Cluster.Inspection
  alias Slap.Cluster.Inspection.Read
  alias Slap.SnapshotLog.{Control, Store}
  alias Slap.SnapshotLog.Introspect.Capture
  alias Slap.Streams.Introspect, as: Streams
  alias Slap.Streams.Introspect.Messages

  def paths(version, base) do
    with :ok <- Inspection.check_version(version), do: {:ok, component_paths(base)}
  end

  def component_paths(base) do
    %{
      control: Store.path(base, :control),
      index: Store.path(base, :index),
      updates: Store.path(base, :updates)
    }
  end

  def capture(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, &Capture.read(&1, &2, opts))
  end

  def scan(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      base = Keyword.fetch!(opts, :base)
      opts = Keyword.put(opts, :prefix, Streams.path_prefix(base <> "/."))

      with {:ok, page} <- Read.page(ctx, deadline, opts, &decode_row/2),
           do: {:ok, Map.put(page, :paths, component_paths(base))}
    end)
  end

  defp decode_row(key, value) do
    with {:ok, %{path: path} = fields} <- Streams.decode(key, value),
         {:ok, component} <- component(path),
         do: {:ok, Map.put(fields, :component, component)}
  end

  def messages(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      with {:ok, page} <- Streams.messages(ctx, deadline, opts) do
        component = Keyword.fetch!(opts, :component)
        base = Keyword.get(opts, :base)
        page = Messages.annotate(page, &interpret(component, &1, base))
        {:ok, selection(page, component, opts)}
      end
    end)
  end

  defp selection(page, :index, opts),
    do: Map.put(page, :current, Capture.index_selection(page, opts))

  defp selection(page, _component, _opts), do: page

  defp interpret(:index, binary, base) when is_binary(base) do
    with {:ok, %{offset: offset} = entry} <- decode(:index, binary),
         do: {:ok, Map.put(entry, :path, Store.path(base, {:snapshot, offset}))}
  end

  defp interpret(component, binary, _base), do: decode(component, binary)

  def component(path) do
    case Store.parse_path(path) do
      {:ok, base, {:snapshot, offset}} ->
        {:ok, %{base: base, component: :snapshot, offset: offset}}

      {:ok, base, component} ->
        {:ok, %{base: base, component: component}}

      :error ->
        {:error, :not_snapshot_log_component}
    end
  end

  def decode(:control, binary) do
    with :error <- Control.parse(binary), do: {:error, :malformed_control}
  end

  def decode(:index, binary) do
    case Store.parse_index_entry(binary) do
      {:ok, entry} -> {:ok, entry}
      :deleted -> {:ok, %{deleted: true}}
      :error -> {:error, :malformed_index}
    end
  end

  def decode(_component, _binary), do: {:error, :opaque_component}
end
