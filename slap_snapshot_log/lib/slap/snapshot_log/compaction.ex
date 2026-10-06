defmodule Slap.SnapshotLog.Compaction do
  @moduledoc false
  # A later entry's `retained` is a subset of snapshots its predecessor
  # names. Once cleanup drops a snapshot, no later entry can name it again.
  # Snapshots above the current offset may belong to active publications.
  # Late writes below it are removed by the next cleanup.

  alias Slap.SnapshotLog.Store

  @type rule :: {every_ms :: pos_integer(), keep_ms :: pos_integer()}

  @spec run(String.t() | Store.t(), non_neg_integer(), binary(), keyword()) ::
          :ok | {:error, term()}
  def run(base, offset, snapshot, opts \\ []) do
    pub = %{
      base: base,
      offset: offset,
      rules: Keyword.get(opts, :history, []),
      now: Keyword.get_lazy(opts, :now, fn -> System.system_time(:millisecond) end),
      after_step: Keyword.get(opts, :after_step, fn _step -> :ok end)
    }

    with {:ok, state} <- Store.state(base), do: publish(pub, snapshot, state)
  end

  defp publish(pub, snapshot, state) do
    case position(pub, state) do
      :current -> cleanup(pub.base, pub.after_step)
      :after -> write(pub, snapshot, state)
      lost -> {:error, lost}
    end
  end

  defp write(%{base: base, offset: offset} = pub, snapshot, state) do
    with :ok <- step(within_tail(base, offset), :check, pub.after_step),
         :ok <- step(Store.snapshot(base, offset, snapshot), :snapshot, pub.after_step),
         do: index(pub, state)
  end

  defp position(_pub, %{deleted: at}) when at != nil, do: :deleted
  defp position(_pub, %{current: nil}), do: :after
  defp position(%{offset: offset}, %{current: %{offset: offset}}), do: :current
  defp position(%{offset: offset}, %{current: %{offset: at}}) when at > offset, do: :superseded
  defp position(_pub, _state), do: :after

  defp within_tail(base, offset) do
    case Store.tail(base) do
      {:ok, tail} when offset <= tail -> :ok
      {:ok, _tail} -> {:error, :offset_beyond_tail}
      {:error, _} = error -> error
    end
  end

  defp index(pub, state) do
    case Store.index(pub.base, entry(pub, state.current), state.index_tail) do
      :ok ->
        pub.after_step.(:index)
        cleanup(pub.base, pub.after_step)

      {:error, :conflict} ->
        retry(pub, state.index_tail)

      {:error, _} = error ->
        error
    end
  end

  defp retry(pub, tried) do
    case Store.state(pub.base) do
      {:ok, %{index_tail: ^tried}} -> {:error, :index_seq_conflict}
      {:ok, state} -> reindex(pub, state)
      {:error, _} = error -> error
    end
  end

  defp reindex(pub, state) do
    case position(pub, state) do
      :current -> cleanup(pub.base, pub.after_step)
      :after -> index(pub, state)
      lost -> lost(pub, state, lost)
    end
  end

  # A superseded publication must remove its own snapshot: cleanup does
  # not run on a deleted log.
  defp lost(pub, state, reason) do
    with :ok <- delete_unless_named(pub.base, pub.offset, state.current),
         :ok <- cleanup(pub.base, pub.after_step),
         do: {:error, reason}
  end

  defp delete_unless_named(base, offset, current) do
    case current != nil and offset in Store.named(current) do
      true -> :ok
      false -> Store.delete_snapshot(base, offset)
    end
  end

  defp entry(pub, previous) do
    new = %{offset: pub.offset, created_at: pub.now}

    older =
      case previous do
        nil -> []
        %{retained: retained} -> [Map.take(previous, [:offset, :created_at]) | retained]
      end

    kept = keep([new | older], pub.rules, pub.now)
    Map.put(new, :retained, Enum.filter(older, &MapSet.member?(kept, &1.offset)))
  end

  @spec keep([Store.snapshot()], [rule()], integer()) :: MapSet.t(non_neg_integer())
  defp keep(snapshots, rules, now) do
    for {every, keep_ms} <- rules,
        {_period, in_period} <- Enum.group_by(snapshots, &div(&1.created_at, every)),
        best = Enum.max_by(in_period, &{&1.created_at, &1.offset}),
        now - best.created_at < keep_ms,
        into: MapSet.new(),
        do: best.offset
  end

  defp cleanup(base, after_step) do
    case Store.state(base) do
      {:ok, %{current: nil}} -> :ok
      {:ok, %{deleted: at}} when at != nil -> :ok
      {:ok, state} -> cleanup(base, state.current, after_step)
      {:error, _} = error -> error
    end
  end

  defp cleanup(base, current, after_step) do
    with {:ok, stored} <- Store.stored(base) do
      named = MapSet.new(Store.named(current))
      unnamed = Enum.filter(stored, &(&1 < current.offset and not MapSet.member?(named, &1)))

      with :ok <-
             step(Store.each_ok(unnamed, &Store.delete_snapshot(base, &1)), :delete, after_step),
           :ok <- Store.trim(base, :updates, current.offset),
           :ok <- Store.trim(base, :index, current.index_offset),
           do: step(:ok, :trim, after_step)
    end
  end

  defp step(:ok, name, after_step) do
    after_step.(name)
    :ok
  end

  defp step({:error, _} = error, _name, _after_step), do: error
end
