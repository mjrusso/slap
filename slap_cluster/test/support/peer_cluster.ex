defmodule Slap.Cluster.Test.TestCluster do
  @moduledoc false
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.Test.Peers do
  @moduledoc false
  # Other BEAM nodes (OS processes) for the multi-node tests, started with
  # :peer on this machine and connected with distributed Erlang.

  alias Slap.Cluster.Test.TestCluster
  alias Slap.SlateDB

  @doc "Makes this node distributed, once."
  def distribute! do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])

      # Long names are the default (the second argument's type changed in
      # Elixir 1.19).
      {:ok, _} = Node.start(:"primary-#{System.unique_integer([:positive])}@127.0.0.1")
    end

    :ok
  end

  @doc """
  Starts a node with this node's code paths and extra VM `args` (charlists).
  Returns `{peer, node}`.
  """
  def start(name, args \\ []),
    do: start_as(:"#{name}-#{System.unique_integer([:positive])}", args)

  @doc "Starts a node as `start/2` does, named exactly `name` (on 127.0.0.1)."
  def start_as(name, args \\ []) do
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1]) ++ args

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie()) | paths],
        connection: :standard_io
      })

    :erpc.call(node, fn ->
      Logger.configure(level: :error)
      {:ok, _} = Application.ensure_all_started(:slap_cluster)
      SlateDB.set_log_level(:none)
    end)

    {peer, node}
  end

  @doc "Starts the lease cluster on `node`, not linked to the caller."
  def start_cluster(node, opts) do
    :erpc.call(node, fn ->
      {:ok, pid} = TestCluster.start_link(opts)
      Process.unlink(pid)
      :ok
    end)
  end

  @doc "Stops the cluster on `node` cleanly."
  def stop_cluster(node), do: :erpc.call(node, TestCluster, :stop, [], 60_000)

  @doc "SIGKILLs the node's OS process."
  def kill(node), do: signal(node, "KILL")

  @doc "SIGSTOP / SIGCONT."
  def pause(node), do: signal(node, "STOP")
  def resume(_node, os_pid), do: System.cmd("kill", ["-CONT", os_pid])

  def os_pid(node), do: :erpc.call(node, System, :pid, [])

  defp signal(node, sig) do
    os_pid = os_pid(node)
    {_, 0} = System.cmd("kill", ["-#{sig}", os_pid])
    os_pid
  end

  def local_shards(node) do
    :erpc.call(node, TestCluster, :local_shards, [], 5_000)
  catch
    _, _ -> :down
  end
end
