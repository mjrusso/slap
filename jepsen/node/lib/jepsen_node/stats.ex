defmodule JepsenNode.Stats do
  @moduledoc """
  Counters for workload coverage. `total/0` sums reachable nodes;
  `collect/2` requires every specified node.
  """

  @names [
    :yjs_compactions,
    :log_snapshots,
    :log_superseded,
    :log_resets,
    :files_inline_writes,
    :files_object_writes
  ]

  @doc "Creates the counters and attaches the telemetry handler."
  @spec setup() :: :ok
  def setup do
    :persistent_term.put(__MODULE__, :counters.new(length(@names), [:write_concurrency]))

    :telemetry.attach(
      __MODULE__,
      [:slap, :yjs, :doc_server, :compact],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  @doc false
  def handle_event(_event, _measurements, _metadata, nil), do: add(:yjs_compactions)

  @spec add(atom()) :: :ok
  def add(name), do: :counters.add(:persistent_term.get(__MODULE__), index(name), 1)

  @spec local() :: %{atom() => non_neg_integer()}
  def local do
    counters = :persistent_term.get(__MODULE__)
    Map.new(@names, &{&1, :counters.get(counters, index(&1))})
  end

  @spec collect([node()], timeout()) :: {:ok, map()} | {:error, term()}
  def collect(nodes, timeout) do
    case :rpc.multicall(nodes, __MODULE__, :local, [], timeout) do
      {counts, []} when length(counts) == length(nodes) -> {:ok, sum(counts)}
      {_counts, unreachable} -> {:error, {:stats_unreachable, unreachable}}
    end
  end

  @spec sum([map()]) :: map()
  def sum(counts),
    do:
      Enum.reduce(counts, Map.new(@names, &{&1, 0}), &Map.merge(&2, &1, fn _k, a, b -> a + b end))

  @doc "Every reachable node's counts, summed."
  @spec total() :: %{atom() => non_neg_integer()}
  def total do
    {counts, _unreachable} =
      :rpc.multicall([node() | Node.list()], __MODULE__, :local, [], 10_000)

    sum(counts)
  end

  defp index(name), do: Enum.find_index(@names, &(&1 == name)) + 1
end
