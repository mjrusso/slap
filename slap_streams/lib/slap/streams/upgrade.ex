defmodule Slap.Streams.Upgrade do
  @moduledoc """
  Offline conversion of stream metadata written by version 0.4.0 and earlier.
  """

  alias Slap.SlateDB
  alias Slap.Streams.Retention
  alias Slap.Streams.Store.{Batch, Codec, Keys, Meta, Read}

  @doc """
  Converts version 1 metadata to version 2 using exclusively owned handles.

  Stop every node using the store and take a backup before opening these
  handles. Only trusted stored ETF records may be supplied. Finish or delete
  incomplete copy forks with the old version before running this conversion.

  Checks all supplied shards before writing and converts metadata in durable
  batches. Stops on the first error; close and reopen the handles before retrying.
  Converted records are skipped. Message rows, offsets, trim, producer state,
  Stream-Seq, expiry and closed state are preserved. Background cleanup finishes
  old deletion and trim work after the cluster restarts.
  """
  @spec run([SlateDB.t()]) :: :ok | {:error, term()}
  def run(dbs) do
    with :ok <- each(dbs, &preflight/1), do: each(dbs, &convert/1)
  rescue
    error in SlateDB.Error -> {:error, error}
  end

  defp rows(db), do: SlateDB.scan(db, prefix: Keys.prefix(:meta), durability: :remote)

  defp preflight(db) do
    each(rows(db), fn row ->
      with {:ok, _ops} <- changes(db, row), do: :ok
    end)
  end

  defp convert(db) do
    db |> rows() |> Stream.chunk_every(100) |> each(&write_page(db, &1))
  end

  defp write_page(db, page) do
    with {:ok, ops} <- page_ops(db, page), do: write(db, ops)
  end

  defp page_ops(db, page) do
    page
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case changes(db, row) do
        {:ok, ops} -> {:cont, {:ok, [ops | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, pages} -> {:ok, pages |> Enum.reverse() |> List.flatten()}
      error -> error
    end
  end

  defp write(_db, []), do: :ok

  defp write(db, ops) do
    with {:ok, _seq} <- SlateDB.write(db, ops), do: SlateDB.flush(db)
  end

  defp changes(db, {key, bytes}) do
    path = Keys.meta_path(key)

    case :erlang.binary_to_term(bytes) do
      {:meta, 1, fields} -> legacy(db, path, fields)
      {:meta, 2, _fields} -> current(bytes)
      _unsupported -> {:error, {:unsupported_metadata, path}}
    end
  end

  defp legacy(db, path, %{copying: false} = fields) do
    meta = struct!(Meta, Map.drop(fields, [:copying, :fork_of, :forks]))
    legacy_ops(db, path, meta)
  end

  defp legacy(_db, path, %{copying: true}), do: {:error, {:incomplete_copy, path}}
  defp legacy(_db, path, _fields), do: {:error, {:unsupported_metadata, path}}

  defp legacy_ops(_db, path, %Meta{soft_deleted: true, sid: sid}) do
    {:ok, Batch.logical_delete(path, sid)}
  end

  defp legacy_ops(db, path, meta) do
    with {:ok, trim} <- Read.get_trim(db, meta.sid) do
      {meta, reclaim} = Retention.authorize(meta, trim)
      {:ok, [{:delete, Keys.repair(meta.sid)} | Batch.put_meta(path, meta) ++ reclaim]}
    end
  end

  defp current(bytes) do
    Codec.decode(bytes)
    {:ok, []}
  end

  defp each(items, fun) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case fun.(item) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
