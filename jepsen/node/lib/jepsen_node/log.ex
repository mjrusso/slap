defmodule JepsenNode.Log do
  @moduledoc """
  The snapshot log workload: a list of integers per key, as a
  `Slap.SnapshotLog` whose entries are the integers and whose snapshots are
  JSON lists. A read rebuilds the list as a new process would: the current
  snapshot, then every entry after it.

  Each node starts followers for keys served by connected nodes
  (`JepsenNode.Log.Follower`). A follower snapshots only entries it has read;
  entries cannot be applied twice. After fault recovery, `restore_followers/1`
  starts followers for retired keys on restarted nodes. `check/1` compares
  every follower's list with the log and reports missing followers and failed reads.
  """

  alias JepsenNode.{Stats, Log.Follower}
  alias Slap.SnapshotLog

  @table __MODULE__
  @publications __MODULE__.Publications
  @catchup_wait_ms 120_000
  @audit_timeout_ms 600_000

  @doc false
  def child_specs do
    :ets.new(@table, [:named_table, :public, :set])
    :ets.new(@publications, [:named_table, :public, :set])

    [
      {Registry, keys: :unique, name: __MODULE__.Registry},
      {DynamicSupervisor, name: __MODULE__.Followers, strategy: :one_for_one},
      {Task.Supervisor, name: __MODULE__.Checks}
    ]
  end

  @spec base(String.t()) :: String.t()
  def base(key), do: "/v1/stream/jepsen/logs/k" <> key

  @doc "Appends `value`. An error means the outcome is unknown."
  @spec append(String.t(), integer()) :: :ok | {:error, term()}
  def append(key, value) do
    follow_everywhere(key)

    case SnapshotLog.append(base(key), Integer.to_string(value)) do
      {:ok, _offset} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc "The list, rebuilt from the current snapshot and the entries after it."
  @spec read(String.t()) :: {:ok, [integer()]} | {:error, term()}
  def read(key) do
    follow_everywhere(key)
    read_log(key)
  end

  defp read_log(key) do
    with {:reset, %{snapshot: snapshot, offset: offset}} <- SnapshotLog.next(base(key), nil) do
      read_entries(base(key), offset, decode(snapshot))
    end
  end

  defp read_entries(base, offset, acc) do
    case SnapshotLog.next(base, offset, wait: 0) do
      {:ok, %{entries: entries, offset: next, up_to_date: up_to_date}} ->
        acc = acc ++ Enum.map(entries, &String.to_integer/1)
        if up_to_date, do: {:ok, acc}, else: read_entries(base, next, acc)

      # Trimmed under the read: start again from the new snapshot.
      {:reset, %{snapshot: snapshot, offset: offset}} ->
        read_entries(base, offset, decode(snapshot))

      {:error, _} = error ->
        error
    end
  end

  @spec decode(binary() | nil) :: [integer()]
  def decode(nil), do: []
  def decode(snapshot), do: JSON.decode!(snapshot)

  @doc false
  def put_follower(key, list), do: :ets.insert(@table, {key, list})

  @doc false
  def record_snapshot(key), do: :ets.insert(@publications, {key})

  @spec check([String.t()]) :: %{
          followers: non_neg_integer(),
          missing: list(),
          lagging: list(),
          mismatches: list(),
          read_errors: list(),
          node: String.t(),
          publications: list(),
          stats: map()
        }
  def check(keys) do
    keys = Enum.map(keys, &to_string/1)
    deadline = System.monotonic_time(:millisecond) + @catchup_wait_ms
    audit_deadline = System.monotonic_time(:millisecond) + @audit_timeout_ms

    results =
      keys
      |> Enum.chunk_every(16)
      |> Enum.flat_map(fn batch ->
        remaining = audit_deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          Enum.map(batch, &read_error(&1, :check_timeout))
        else
          __MODULE__.Checks
          |> Task.Supervisor.async_stream_nolink(
            batch,
            fn key ->
              case Registry.lookup(__MODULE__.Registry, key) do
                [_] -> check_follower(key, deadline)
                [] -> {:missing, key}
              end
            end,
            max_concurrency: 16,
            timeout: min(60_000, remaining),
            on_timeout: :kill_task
          )
          |> Enum.zip(batch)
          |> Enum.map(fn
            {{:ok, result}, _key} -> result
            {{:exit, reason}, key} -> read_error(key, reason)
          end)
        end
      end)

    %{
      followers: length(keys) - Enum.count(results, &match?({:missing, _}, &1)),
      missing: for({:missing, key} <- results, do: %{node: to_string(node()), key: key}),
      lagging: for({:lagging, value} <- results, do: value),
      mismatches: for({:mismatch, value} <- results, do: value),
      read_errors: for({:read_error, value} <- results, do: value),
      node: to_string(node()),
      publications: for({key} <- :ets.tab2list(@publications), do: key),
      stats: Stats.local()
    }
  end

  defp check_follower(key, deadline) do
    case read_log(key) do
      {:ok, log} ->
        await_follower(key, log, deadline)

      {:error, reason} ->
        read_error(key, reason)
    end
  end

  defp await_follower(key, log, deadline) do
    list =
      case :ets.lookup(@table, key) do
        [{^key, value}] -> value
        [] -> nil
      end

    cond do
      Registry.lookup(__MODULE__.Registry, key) == [] ->
        {:missing, key}

      list == log ->
        :caught_up

      is_list(list) and not List.starts_with?(log, list) ->
        case read_log(key) do
          {:ok, current_log} ->
            if List.starts_with?(current_log, list) do
              await_follower(key, current_log, deadline)
            else
              {:mismatch, %{node: to_string(node()), key: key, follower: list, log: current_log}}
            end

          {:error, reason} ->
            read_error(key, reason)
        end

      System.monotonic_time(:millisecond) >= deadline ->
        {:lagging, %{node: to_string(node()), key: key, follower: list, log: log}}

      true ->
        Process.sleep(100)
        await_follower(key, log, deadline)
    end
  end

  defp read_error(key, reason),
    do: {:read_error, %{node: to_string(node()), key: key, reason: inspect(reason)}}

  defp follow_everywhere(key) do
    start_follower(key)
    Enum.each(Node.list(), &:rpc.cast(&1, __MODULE__, :start_follower, [key]))
  end

  @spec restore_followers([String.t()]) :: :ok | {:error, term()}
  def restore_followers(keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case start_follower(to_string(key)) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  def start_follower(key) do
    case Registry.lookup(__MODULE__.Registry, key) do
      [_] ->
        :ok

      [] ->
        case DynamicSupervisor.start_child(__MODULE__.Followers, {Follower, key}) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, _} = error -> error
        end
    end
  end
end
