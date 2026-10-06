defmodule Slap.SlateDB.Telemetry do
  @moduledoc """
  Emits `:telemetry` events for one database. Start it after `Slap.SlateDB.open/2`,
  usually under the supervisor that owns the database:

      {:ok, db} = Slap.SlateDB.open("orders", store: store)
      {:ok, _pid} = Slap.SlateDB.Telemetry.start_link(db: db, metadata: %{name: "orders"})

  Events, each with the `:metadata` option as metadata:

    * `[:slap, :slatedb, :durable]` - `%{durable_seq: seq}`, when the durable
      sequence number goes up. Updates are coalesced, as for
      `Slap.SlateDB.subscribe/3`.
    * `[:slap, :slatedb, :stats]` - `Slap.SlateDB.stats/1`, every `:interval` ms
      (default 10,000).
    * `[:slap, :slatedb, :closed]` - `%{}`, with `:reason` added to the metadata
      (`:clean`, `:fenced`, `:panic` or `:unknown`), once, when the database
      closes. The process then stops normally.

  Options: `:db` (required), `:metadata` (a map, default `%{}`), `:interval`.
  A supervised child uses its database handle as part of its child ID, so
  one supervisor can run telemetry for multiple databases.
  """

  use GenServer, restart: :transient

  alias Slap.SlateDB

  @default_interval 10_000

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    validate_options!(opts)

    %{
      id: {__MODULE__, Keyword.fetch!(opts, :db)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    validate_options!(opts)
    GenServer.start_link(__MODULE__, opts)
  end

  defp validate_options!(opts) do
    Keyword.validate!(opts, [:db, :metadata, :interval])
    unless Keyword.has_key?(opts, :db), do: raise(ArgumentError, ":db is required")

    unless is_map(Keyword.get(opts, :metadata, %{})),
      do: raise(ArgumentError, ":metadata must be a map")

    interval = Keyword.get(opts, :interval, @default_interval)

    unless is_integer(interval) and interval > 0,
      do: raise(ArgumentError, ":interval must be a positive integer")
  end

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    {:ok, subscription} = SlateDB.subscribe(db, __MODULE__)

    state = %{
      db: db,
      ref: subscription.ref,
      metadata: Keyword.get(opts, :metadata, %{}),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule_stats(state)
    {:ok, state}
  end

  @impl true
  def handle_info({:slap_slatedb_durable, ref, __MODULE__, seq}, %{ref: ref} = state) do
    :telemetry.execute([:slap, :slatedb, :durable], %{durable_seq: seq}, state.metadata)
    {:noreply, state}
  end

  def handle_info({:slap_slatedb_closed, ref, __MODULE__, reason}, %{ref: ref} = state) do
    :telemetry.execute([:slap, :slatedb, :closed], %{}, Map.put(state.metadata, :reason, reason))
    {:stop, :normal, state}
  end

  def handle_info(:stats, state) do
    :telemetry.execute([:slap, :slatedb, :stats], SlateDB.stats(state.db), state.metadata)
    schedule_stats(state)
    {:noreply, state}
  end

  defp schedule_stats(state), do: Process.send_after(self(), :stats, state.interval)
end
