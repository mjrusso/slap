defmodule Slap.Cluster.Shard do
  @moduledoc """
  The context of a shard placed on this node, given to the application's
  shard children and to functions run with `call/3`.

    * `:cluster` - the cluster module.
    * `:n` - the shard number, `0..shards - 1`.
    * `:db` - the open `Slap.SlateDB` handle. Read and write through it directly.
    * `:registry` - the cluster's `Registry`. Name per-shard processes with
      the cluster's `via/2`, which scopes names to `{n, name}`.
    * `:generation` - the strategy's lease generation, or `nil` (`Local`).
    * `:child_options` - application settings passed to shard children.

  `:status` and `:shard_db` are internal coordination fields. Use
  `Slap.Cluster.shard_status/1` to inspect the shard's state.

  A context is only valid while the shard is open on this node. When the
  shard stops, `Slap.Cluster.shard_status/1` says why.
  """

  alias Slap.SlateDB

  @enforce_keys [:cluster, :n, :db, :registry, :generation, :child_options, :status, :shard_db]
  defstruct [:cluster, :n, :db, :registry, :generation, :child_options, :status, :shard_db]

  @type t :: %__MODULE__{
          cluster: module(),
          n: non_neg_integer(),
          db: SlateDB.t(),
          registry: atom(),
          generation: non_neg_integer() | nil,
          child_options: keyword(),
          status: :atomics.atomics_ref(),
          shard_db: pid()
        }

  # The status is an atomics array shared by the host, which sets it before
  # stopping a shard, and everything that holds the context.
  @doc false
  def new_status, do: :atomics.new(1, [])

  @statuses %{0 => :running, 1 => :stopping, 2 => :fenced}
  @codes Map.new(@statuses, fn {k, v} -> {v, k} end)

  @doc false
  def status(ref), do: Map.fetch!(@statuses, :atomics.get(ref, 1))

  @doc false
  def put_status(ref, status), do: :atomics.put(ref, 1, Map.fetch!(@codes, status))

  @doc false
  # A running shard becomes stopping; a fenced one stays fenced, so its
  # children can tell a fence from a shutdown.
  def mark_stopping(ref) do
    :atomics.compare_exchange(ref, 1, @codes.running, @codes.stopping)
    :ok
  end
end
