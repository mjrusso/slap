defmodule Slap.Cluster.Strategy do
  @moduledoc """
  Decides which node owns which shard.

  A strategy is a process started under the cluster's supervisor, after the
  host. It starts and stops shards on this node only through
  `Slap.Cluster.Host.start_shard/3` and `stop_shard/2`, and the host
  reports every shard that stops through `c:handle_shard_down/3`.

  SlateDB's fencing keeps at most one effective writer per shard whatever
  the strategy does, so a strategy only has to be mostly right: two nodes
  that both open a shard cost errors and a restart, not data.
  """

  @type shard :: non_neg_integer()
  @type location :: {:local, pid()} | {:remote, node()}
  @type reason :: :fenced | :crashed | :stopped

  @doc "The strategy's child spec. `opts` includes `:cluster`."
  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()

  @doc """
  Where `shard` is owned, as far as the strategy knows. The result may be
  stale.
  """
  @callback lookup(cluster :: module(), shard) :: {:ok, location} | {:error, :unassigned}

  @doc """
  Called by the host, from the host process, when a shard on this node
  stops: `:fenced` (another writer opened it), `:crashed` (it failed more
  often than its supervisor allows) or `:stopped` (`stop_shard/2`). Must
  not block; send a message to the strategy's process instead.
  """
  @callback handle_shard_down(cluster :: module(), shard, reason) :: :ok

  @doc """
  Refreshes the strategy's view of where shards are, after `call/4` found
  it stale. Optional. It must return within `timeout` ms.
  """
  @callback refresh(cluster :: module(), timeout :: timeout()) :: :ok

  @doc """
  Validates strategy options before the cluster starts. Raise `ArgumentError`
  for invalid options. Optional for custom strategies.
  """
  @callback validate_options(opts :: keyword()) :: :ok

  @optional_callbacks refresh: 2, validate_options: 1

  @doc false
  def validate_options!(opts, allowed, positive \\ [], nonnegative \\ []) do
    Keyword.validate!(opts, allowed)

    for key <- positive, Keyword.has_key?(opts, key), not positive_integer?(opts[key]) do
      raise ArgumentError,
            "#{inspect(key)} must be a positive integer, got: #{inspect(opts[key])}"
    end

    for key <- nonnegative,
        Keyword.has_key?(opts, key),
        not (is_integer(opts[key]) and opts[key] >= 0) do
      raise ArgumentError,
            "#{inspect(key)} must be a nonnegative integer, got: #{inspect(opts[key])}"
    end

    :ok
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0
end
