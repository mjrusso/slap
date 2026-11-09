defmodule Slap.Inspect.Links do
  @moduledoc false

  # `{:ok, view, opts, shard}` follows a link in the current cluster;
  # a nil shard keeps the current shard. `{:open, cluster, view, shard}`
  # opens a view in the named cluster, selecting `shard` when given.
  # `:none` means the row has no link.

  alias Slap.Inspect.ShardRows

  def follow(%{decoded: {:ok, %{kind: :cluster_view} = listing}}, _request),
    do: {:open, listing.cluster, listing.view, holding(listing.counts)}

  def follow(%{decoded: {:ok, %{kind: :registration, value: %{target: target}}}}, _request),
    do: {:ok, :logical, [path: target.path, expected_sid: target.sid], target.shard}

  def follow(%{decoded: {:ok, %{value: %{body: [:object, key]}}}}, request) do
    opts = Keyword.take(request.opts, [:instance])
    {:ok, :object, Keyword.put(opts, :object_key, key), nil}
  end

  def follow(%{decoded: {:ok, %{kind: :log, base: base}}}, _request),
    do: {:ok, :log, [base: base], nil}

  def follow(%{decoded: {:ok, %{kind: :yjs_document, base: base}}}, _request),
    do: {:ok, :yjs, [base: base], nil}

  def follow(%{decoded: {:ok, %{kind: :partition, partition: partition}}}, _request),
    do: {:ok, :kv, [partition: partition], nil}

  def follow(%{decoded: {:ok, %{kind: :files_partition, partition: partition}}}, request),
    do: {:ok, :files, [partition: partition] ++ Keyword.take(request.opts, [:instance]), nil}

  def follow(%{decoded: {:ok, %{path: path}}}, _request), do: {:ok, :logical, [path: path], nil}

  def follow(%{decoded: {:ok, %{value: %{source: source}}}}, _request),
    do: {:ok, :logical, [path: source.path, expected_sid: source.sid], source.shard}

  def follow(_row, _request), do: :none

  defp holding(nil), do: nil

  defp holding(counts),
    do: Enum.find_value(counts, &(ShardRows.rows?(&1.rows) && &1.shard))

  def retention(%{decoded: {:ok, %{path: path, value: %{sid: sid}}}}),
    do: {:ok, :retention, [path: path, sid: sid], nil}

  def retention(_row), do: :none
end
