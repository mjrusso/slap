defmodule Slap.Cluster.Readiness do
  @moduledoc false

  alias Slap.Cluster.{Config, ShardSupervisor}

  # A stalled owner must not prevent rediscovery after placement changes.
  @snapshot_timeout 1_000
  @inspection_timeout 100
  @inspection_concurrency 8

  def status(cluster, timeout), do: query(cluster, deadline(timeout), :status)

  defp query(cluster, deadline, mode),
    do: run(fn -> snapshot(cluster, deadline, mode) end, deadline)

  defp run(fun, deadline) do
    task = Task.async(fun)

    case Task.yield(task, remaining(deadline)) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
    end
  end

  def await_ready(cluster, timeout), do: await(cluster, deadline(timeout))

  defp await(cluster, deadline) do
    attempt_deadline = deadline(min(@snapshot_timeout, remaining(deadline)))

    cond do
      remaining(deadline) == 0 ->
        {:error, :timeout}

      query(cluster, attempt_deadline, :wait) == :ok ->
        :ok

      true ->
        Process.sleep(min(25, remaining(deadline)))
        await(cluster, deadline)
    end
  end

  defp snapshot(cluster, deadline, mode) do
    routes = Map.new(0..(Config.get(cluster).shards - 1), fn n -> {n, owner(cluster, n)} end)
    owners = Enum.uniq([node() | Map.values(routes)])
    host = Slap.Cluster.host(cluster)

    hosts =
      query_owners(owners, fn owner ->
        {owner,
         call_owner(owner, fn timeout -> GenServer.call(host, :status, timeout) end, deadline)}
      end)

    with {:ok, _local} <- hosts[node()] do
      if mode == :wait and not all_shards?(routes, hosts, &match?({:running, _}, &1)) do
        {:error, :not_ready}
      else
        result(routes, inspect_hosts(routes, hosts, deadline), mode)
      end
    end
  rescue
    ArgumentError -> {:error, :unavailable}
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  defp result(routes, hosts, :wait) do
    if all_shards?(routes, hosts, &(&1 == :running)), do: :ok, else: {:error, :not_ready}
  end

  defp result(routes, hosts, :status) do
    with {:ok, local} <- hosts[node()] do
      shards = Map.new(routes, fn {n, owner} -> {n, shard_status(hosts[owner], owner, n)} end)
      ready? = Enum.all?(shards, fn {_, shard} -> shard.status == :running end)
      {:ok, %{ready?: ready?, probe: local.probe, shards: shards}}
    end
  end

  defp all_shards?(routes, hosts, predicate) do
    Enum.all?(routes, fn {n, owner} ->
      case hosts[owner] do
        {:ok, host} -> predicate.(Map.get(host.shards, n))
        {:error, _} -> false
      end
    end)
  end

  defp query_owners(owners, fun) do
    owners
    |> Task.async_stream(fun,
      max_concurrency: Enum.count(owners),
      ordered: false,
      timeout: :infinity
    )
    |> Map.new(fn {:ok, result} -> result end)
  end

  defp inspect_hosts(routes, hosts, deadline) do
    routes
    |> Enum.group_by(fn {_n, owner} -> owner end, fn {n, _owner} -> n end)
    |> Map.put_new(node(), [])
    |> query_owners(fn {owner, shards} ->
      {owner, inspect_owner(owner, shards, hosts[owner], deadline)}
    end)
  end

  defp inspect_owner(_owner, _shards, {:error, _} = error, _deadline), do: error

  defp inspect_owner(owner, shards, {:ok, host}, deadline) do
    running = for n <- shards, {:running, pid} <- [host.shards[n]], do: {n, pid}

    inspection =
      case running do
        [] -> {:ok, %{}}
        _ -> call_owner(owner, fn timeout -> inspect_shards(running, timeout) end, deadline)
      end

    with {:ok, statuses} <- inspection do
      {:ok, %{host | shards: Map.merge(host.shards, statuses)}}
    end
  end

  defp owner(cluster, n) do
    case Slap.Cluster.lookup(cluster, n) do
      {:ok, {:remote, owner}} -> owner
      _ -> node()
    end
  end

  defp shard_status({:ok, host}, owner, n) do
    %{
      node: owner,
      status: Map.get(host.shards, n, :unassigned),
      last_error: Map.get(host.failures, n)
    }
  end

  defp shard_status({:error, reason}, owner, _n),
    do: %{node: owner, status: :unavailable, last_error: reason}

  @doc false
  def inspect_shards(shards, timeout) do
    deadline = deadline(timeout)

    case run(fn -> do_inspect(shards, deadline) end, deadline) do
      {:error, :timeout} -> Map.new(shards, fn {n, _pid} -> {n, :starting} end)
      statuses -> statuses
    end
  end

  defp do_inspect(shards, deadline) do
    inspections =
      Task.async_stream(shards, fn {_n, pid} -> ShardSupervisor.status(pid) end,
        max_concurrency: @inspection_concurrency,
        timeout: min(@inspection_timeout, remaining(deadline)),
        on_timeout: :kill_task
      )

    shards
    |> Enum.zip_with(inspections, fn
      {n, _}, {:ok, status} -> {n, status}
      {n, _}, {:exit, _} -> {n, :starting}
    end)
    |> Map.new()
  end

  defp call_owner(owner, operation, deadline) do
    timeout = remaining(deadline)

    result =
      if owner == node() do
        operation.(timeout)
      else
        :erpc.call(owner, fn -> operation.(timeout) end, timeout)
      end

    {:ok, result}
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :error, {:erpc, reason} -> {:error, {:erpc, reason}}
    :exit, _reason -> {:error, :unavailable}
  end

  defp deadline(:infinity), do: :infinity

  defp deadline(timeout) when is_integer(timeout) and timeout >= 0,
    do: System.monotonic_time(:millisecond) + timeout

  defp deadline(_timeout),
    do: raise(ArgumentError, "timeout must be nonnegative milliseconds or :infinity")

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
