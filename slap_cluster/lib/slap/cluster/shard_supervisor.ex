defmodule Slap.Cluster.ShardSupervisor do
  @moduledoc false
  # Stop application children before closing the database. Placement controls
  # shard restarts; reopening here could fence a new owner.

  use Supervisor

  alias Slap.Cluster.{Config, ShardDb}

  def child_spec({_cluster, n, _generation, _status} = arg) do
    %{
      id: {__MODULE__, n},
      start: {__MODULE__, :start_link, [arg]},
      type: :supervisor,
      restart: :temporary
    }
  end

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg)

  # Inspection can block during child startup; callers must bound it outside Host.
  def status(pid) do
    if Enum.all?(Supervisor.which_children(pid), &running_child?/1),
      do: :running,
      else: :starting
  catch
    :exit, _ -> :starting
  end

  @impl true
  def init({cluster, n, _generation, _status} = arg) do
    config = Config.get(cluster)
    shard_db = Supervisor.child_spec({ShardDb, arg}, restart: :temporary, significant: true)
    children = [shard_db] ++ children(config, cluster, n)
    Supervisor.init(children, strategy: :rest_for_one, auto_shutdown: :any_significant)
  end

  defp children(%Config{shard_children: nil}, _cluster, _n), do: []

  defp children(_config, cluster, n) do
    [
      %{
        id: __MODULE__.Children,
        start: {__MODULE__, :start_children, [cluster, n]},
        type: :supervisor,
        restart: :temporary,
        significant: true
      }
    ]
  end

  @doc false
  def start_children(cluster, n) do
    [{_pid, ctx}] = Registry.lookup(Slap.Cluster.registry(cluster), {:shard, n})

    specs =
      case Config.get(cluster).shard_children do
        {m, f, a} -> apply(m, f, [ctx | a])
        fun when is_function(fun, 1) -> fun.(ctx)
      end

    Supervisor.start_link(specs, strategy: :one_for_one, auto_shutdown: :any_significant)
  end

  defp running_child?({__MODULE__.Children, pid, :supervisor, _}) do
    is_pid(pid) and Process.alive?(pid) and
      Enum.all?(Supervisor.which_children(pid), &running_child?/1)
  end

  # OTP retains ignored and stopped child specifications with an undefined PID.
  defp running_child?({_id, :undefined, _type, _modules}), do: true
  defp running_child?({_id, pid, _type, _modules}) when is_pid(pid), do: Process.alive?(pid)
  defp running_child?(_child), do: false
end
