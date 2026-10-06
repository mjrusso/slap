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

  @doc "Runs until nothing is left to delete (for tests)."
  def drain(ctx), do: GenServer.call(name(ctx), :drain, :infinity)

  @impl true
  def init(ctx) do
    interval = Keyword.get(ctx.child_options, :deleter_interval, @default_interval)
    page = Keyword.get(ctx.child_options, :deleter_page, @default_page)
    Process.send_after(self(), :tick, interval)
    # `trimmed`: sid => the trim point already deleted up to.
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
    pending = pending(state.ctx.db, 0x06)
    Streams.ShardLoad.set_backlog(state.ctx, :delete, length(pending))
    Enum.each(pending, &delete_stream(state, &1))
    Streams.ShardLoad.set_backlog(state.ctx, :delete, 0)
    trim(state)
  rescue
    error ->
      Logger.error("deleter: #{Exception.message(error)}")
      state
  end

  # Only durable markers: a delete or trim that is not durable yet may still
  # be lost in a crash, and then its rows must still be there. (Readers see
  # the delete or trim only once it is durable, too.)
  defp pending(db, type) do
    db
    |> SlateDB.scan(prefix: Keys.type_prefix(type), durability: :remote)
    |> Enum.to_list()
  end

  # Deletes the stream's messages from the cursor on, a page at a time, the
  # new cursor in the same batch as the page, then the rest of its rows.
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
            producers ++ [{:delete, Keys.tail(sid)}, {:delete, Keys.trim(sid)}, {:delete, key}]
        )
    end
  end

  # The stream server owns each trim marker, so page progress stays in
  # memory. After a restart, rescanning earlier pages finds tombstones.
  defp trim(state) do
    %{ctx: %{db: db}, page: page} = state

    trimmed =
      for {key, <<offset::64>>} <- pending(db, 0x07), into: %{} do
        sid = Keys.decode_sid(key)
        if Map.get(state.trimmed, sid) != offset, do: trim_below(db, sid, 0, offset, page)
        {sid, offset}
      end

    %{state | trimmed: trimmed}
  end

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
