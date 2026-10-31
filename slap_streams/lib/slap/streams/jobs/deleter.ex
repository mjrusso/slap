defmodule Slap.Streams.Jobs.Deleter do
  @moduledoc false

  use Slap.Streams.ShardProcess, name: :deleter
  require Logger

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Store.Keys

  @default_interval 1_000
  @default_page 10_000
  @max_offset 0xFFFFFFFFFFFFFFFF

  @doc "Runs one pass over durable deletion and trim work."
  def drain(ctx, timeout \\ :infinity), do: GenServer.call(name(ctx), :drain, timeout)

  @impl true
  def init(ctx) do
    interval = Keyword.get(ctx.child_options, :deleter_interval, @default_interval)
    page = Keyword.get(ctx.child_options, :deleter_page, @default_page)
    Process.send_after(self(), :tick, interval)
    {:ok, %{ctx: ctx, interval: interval, page: page, trimmed: %{}}}
  end

  @impl true
  def handle_call(:drain, _from, state) do
    # Markers are acted on once durable: make them durable now.
    :ok = SlateDB.flush(state.ctx.db)
    {:reply, :ok, run(state)}
  end

  @impl true
  def handle_cast(:kick, state), do: {:noreply, run(state)}

  @impl true
  def handle_info(:tick, state) do
    state = run(state)
    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end

  defp run(state) do
    pending = pending(state.ctx.db, :delete_pending)
    Streams.ShardLoad.set_backlog(state.ctx, :delete, length(pending))
    Enum.each(pending, &delete_stream(state, &1))
    Streams.ShardLoad.set_backlog(state.ctx, :delete, 0)
    trim(state)
  rescue
    error ->
      Logger.error("deleter: #{Exception.message(error)}")
      state
  end

  # A non-durable marker may disappear after a crash. Deleting its rows
  # would then lose acknowledged data.
  defp pending(db, kind) do
    db
    |> SlateDB.scan(prefix: Keys.prefix(kind), durability: :remote)
    |> Enum.to_list()
  end

  defp delete_stream(state, {key, <<cursor::64>>}) do
    %{ctx: %{db: db}, page: page} = state
    sid = Keys.decode_sid(key)

    case page(db, sid, cursor, @max_offset, page) do
      {:full, ops, last} ->
        write!(db, [{:put, key, <<last::64>>} | ops])
        delete_stream(state, {key, <<last::64>>})

      {:last, ops} ->
        producers =
          for {k, _} <- SlateDB.scan(db, prefix: Keys.producer_prefix(sid)), do: {:delete, k}

        write!(
          db,
          ops ++
            producers ++
            [
              {:delete, Keys.tail(sid)},
              {:delete, Keys.trim(sid)},
              {:delete, Keys.reclaim(sid)},
              {:delete, key}
            ]
        )
    end
  end

  # A stream server can advance the deletion bound while this pass runs.
  # Keeping progress in memory prevents overwriting that newer bound.
  defp trim(state) do
    %{ctx: %{db: db}, page: page} = state

    trimmed =
      for {key, <<offset::64>>} <- pending(db, :reclaim), into: %{} do
        sid = Keys.decode_sid(key)
        if Map.get(state.trimmed, sid) != offset, do: trim_below(db, sid, 0, offset, page)
        {sid, offset}
      end

    %{state | trimmed: trimmed}
  end

  defp trim_below(_db, _sid, from, until, _page) when from >= until, do: :ok

  defp trim_below(db, sid, from, until, page) do
    case page(db, sid, from, until, page) do
      {:full, ops, last} ->
        write!(db, ops)
        trim_below(db, sid, last, until, page)

      {:last, ops} ->
        write!(db, ops)
    end
  end

  # The deletes for up to `size` message rows of `sid` in `from..until - 1`:
  # `{:full, ops, last}` for a full page, where `last` is the offset to go on
  # from (a message's parts may span pages; deleted rows are not scanned
  # again), or `{:last, ops}`.
  defp page(db, sid, from, until, size) do
    rows = db |> SlateDB.scan(Keys.msg_range(sid, from, until)) |> Enum.take(size)
    ops = for {k, _} <- rows, do: {:delete, k}

    if length(rows) == size do
      {last, _part} = rows |> List.last() |> elem(0) |> Keys.decode_msg()
      {:full, ops, last}
    else
      {:last, ops}
    end
  end

  defp write!(_db, []), do: :ok

  defp write!(db, ops) do
    case SlateDB.write(db, ops) do
      {:ok, _} -> :ok
      {:error, error} -> raise error
    end
  end
end
