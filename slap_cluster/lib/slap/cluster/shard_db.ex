defmodule Slap.Cluster.ShardDb do
  @moduledoc false
  # Owns a shard's SlateDB handle: opens it, holds the shard's one durability
  # subscription, fans it out to `notify_when_durable/3` waiters, reports a
  # fence to the host, and closes the database when the shard stops.
  #
  # It is the first child of the shard supervisor (rest_for_one), so the
  # application's children start after the database is open and stop before
  # it is closed.

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Shard, Telemetry}
  alias Slap.SlateDB

  def child_spec({cluster, n, _generation, _status} = arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      # Closing flushes the WAL and memtable to object storage.
      shutdown: Config.get(cluster).close_timeout,
      meta: n
    }
  end

  def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

  @doc false
  # Waiters are served by this process, in seq order, so one process gets
  # its notifications in the order of their seqs.
  def notify_when_durable(%Shard{shard_db: pid}, seq, tag, dest),
    do: GenServer.cast(pid, {:notify, seq, dest, tag})

  @impl true
  def init({cluster, n, generation, status}) do
    Process.flag(:trap_exit, true)
    config = Config.get(cluster)
    path = Config.shard_path(config, n)

    open_opts =
      [store: config.store, settings: config.settings]
      |> put_if(:cache, config.db_cache)
      |> put_if(:merge_operator, config.merge_operator)

    case SlateDB.open(path, open_opts) do
      {:ok, db} ->
        {:ok, subscription} = SlateDB.subscribe(db, {:shard, n})

        ctx = %Shard{
          cluster: cluster,
          n: n,
          db: db,
          registry: Slap.Cluster.registry(cluster),
          generation: generation,
          child_options: config.child_options,
          status: status,
          shard_db: self()
        }

        # Registry replaces the entry of a previous ShardDb for this shard
        # that has exited but not yet been cleaned up.
        # The status is not set here: it is :running from its creation, and
        # the host may have set it to :stopping since.
        {:ok, _} = Registry.register(ctx.registry, {:shard, n}, ctx)

        Telemetry.execute([:shard, :start], %{}, %{
          cluster: cluster,
          shard: n,
          generation: generation
        })

        schedule_lag(config)

        {:ok,
         %{
           ctx: ctx,
           config: config,
           sub_ref: subscription.ref,
           waiters: :gb_trees.empty(),
           fenced: false
         }}

      {:error, error} ->
        {:stop, {:open_failed, path, error}}
    end
  end

  @impl true
  def handle_cast({:notify, seq, dest, tag}, state) do
    key = {seq, System.unique_integer([:monotonic])}
    waiters = :gb_trees.insert(key, {dest, tag}, state.waiters)
    # Read the durable seq from SlateDB rather than the last notification,
    # which may still be in the mailbox.
    {:noreply, %{state | waiters: release(waiters, SlateDB.durable_seq(state.ctx.db))}}
  end

  @impl true
  def handle_info({:slap_slatedb_durable, ref, {:shard, _}, durable}, %{sub_ref: ref} = state) do
    {:noreply, %{state | waiters: release(state.waiters, durable)}}
  end

  def handle_info(
        {:slap_slatedb_closed, ref, {:shard, n}, :fenced},
        %{sub_ref: ref, fenced: false} = state
      ) do
    %{ctx: ctx} = state
    Shard.put_status(ctx.status, :fenced)

    Logger.warning(
      "#{inspect(ctx.cluster)}: shard #{n} was fenced: another writer opened its database"
    )

    Telemetry.execute([:shard, :fenced], %{}, %{
      cluster: ctx.cluster,
      shard: n,
      generation: ctx.generation
    })

    # The host stops the shard, application children first. Waiters are
    # dropped: their writes will never be reported durable here.
    send(Slap.Cluster.host(ctx.cluster), {:shard_fenced, n, self()})
    {:noreply, %{state | fenced: true, waiters: :gb_trees.empty()}}
  end

  def handle_info({:slap_slatedb_closed, ref, {:shard, _}, :fenced}, %{sub_ref: ref} = state),
    do: {:noreply, state}

  # Only this process closes the database, in terminate/2, so any other
  # close is unexpected. Exiting stops the shard (a significant child).
  def handle_info({:slap_slatedb_closed, ref, {:shard, _}, reason}, %{sub_ref: ref} = state),
    do: {:stop, {:db_closed, reason}, state}

  def handle_info(:sample_lag, state) do
    %{ctx: ctx} = state

    Telemetry.execute([:durability, :lag], %{lag: SlateDB.durability_lag(ctx.db)}, %{
      cluster: ctx.cluster,
      shard: ctx.n
    })

    schedule_lag(state.config)
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(reason, %{ctx: ctx}) do
    case SlateDB.close(ctx.db) do
      :ok ->
        :ok

      # Close found the fence: writes not yet durable are lost. None of them
      # were reported durable, so no acknowledged write is lost.
      {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} ->
        Shard.put_status(ctx.status, :fenced)

      {:error, error} ->
        Logger.error(
          "#{inspect(ctx.cluster)}: closing shard #{ctx.n} failed: #{Exception.message(error)}"
        )
    end

    Telemetry.execute([:shard, :stop], %{}, %{
      cluster: ctx.cluster,
      shard: ctx.n,
      generation: ctx.generation,
      reason: reason,
      status: Shard.status(ctx.status)
    })
  end

  # Sends every waiter with seq <= durable its notification, lowest seq
  # first.
  defp release(waiters, durable) do
    if :gb_trees.is_empty(waiters) do
      waiters
    else
      case :gb_trees.smallest(waiters) do
        {{seq, _} = key, {dest, tag}} when seq <= durable ->
          send(dest, {:slap_cluster_durable, tag})
          release(:gb_trees.delete(key, waiters), durable)

        _ ->
          waiters
      end
    end
  end

  defp schedule_lag(config), do: Process.send_after(self(), :sample_lag, config.lag_interval)

  defp put_if(opts, _key, nil), do: opts
  defp put_if(opts, key, value), do: Keyword.put(opts, key, value)
end
