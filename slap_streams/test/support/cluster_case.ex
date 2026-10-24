defmodule Slap.Streams.Test.ClusterCase do
  @moduledoc false

  alias Slap.Streams
  alias Slap.Streams.Jobs.Repair
  alias Slap.Streams.Store.Read

  use ExUnit.CaseTemplate

  using do
    quote do
      import Slap.Streams.Test.ClusterCase
      alias Slap.Streams
    end
  end

  setup context do
    dir = Path.join(System.tmp_dir!(), "slap-streams-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    settings =
      Map.merge(
        %{flush_interval: "2ms", manifest_poll_interval: "100ms"},
        context[:settings] || %{}
      )

    opts = [
      store: {:local, dir},
      shards: context[:shards] || 2,
      settings: settings,
      child_options: context[:child_options] || []
    ]

    start_supervised!({Streams.Cluster, opts})

    %{dir: dir, cluster_opts: opts}
  end

  @doc "The context of `path`'s shard (a single-node test cluster has every shard open)."
  def ctx_for(path) do
    {:ok, {:local, ctx}} =
      Streams.Cluster.lookup(Streams.Cluster.shard_for(Streams.placement_key(path)))

    ctx
  end

  @doc "The stream server of `path`, if running."
  def server(path) do
    ctx = ctx_for(path)

    case Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc "The messages' bytes from a read."
  def bodies(%{messages: messages}), do: Enum.map(messages, &elem(&1, 1))

  def retention_counts do
    for ctx <- contexts(),
        {path, meta} <- Read.list_meta(ctx.db, "", durability: :remote),
        into: %{},
        do: {path, meta.registration_count}
  end

  @doc "Stops repair dispatch and waits for existing checks on every local shard."
  def suspend_repair do
    jobs =
      for ctx <- contexts(),
          do: GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :repair))

    # Workers can notify other shards, so stop all dispatch before waiting.
    Enum.each(jobs, &:sys.suspend(&1, 30_000))

    workers =
      for job <- jobs,
          {:process, worker} <- elem(Process.info(job, :monitors), 1),
          do: {worker, Process.monitor(worker)}

    for {worker, ref} <- workers do
      assert_receive {:DOWN, ^ref, :process, ^worker, reason}, 30_000
      assert reason in [:normal, :noproc]
    end

    :ok
  end

  def resume_repair do
    for ctx <- contexts() do
      :sys.resume(Slap.Cluster.via(Streams.Cluster, ctx.n, :repair), 30_000)
    end

    :ok
  end

  def repair_until(expected, timeout \\ 5_000) do
    task = Task.async(fn -> await_cleanup(expected) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, :ok} ->
        :ok

      nil ->
        flunk(
          "durable cleanup condition not reached: #{inspect(expected)}; retention: #{inspect(retention_counts())}"
        )
    end
  end

  defp await_cleanup(expected) do
    for ctx <- contexts(), do: Repair.sweep(ctx)
    if cleanup_complete?(expected), do: :ok, else: await_cleanup(expected)
  end

  defp cleanup_complete?(condition) when is_function(condition, 0), do: condition.()

  defp cleanup_complete?(counts) do
    actual = retention_counts()
    Enum.all?(counts, fn {path, count} -> Map.get(actual, path) == count end)
  end

  def contexts do
    for n <- Streams.Cluster.local_shards() do
      {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
      ctx
    end
  end
end
