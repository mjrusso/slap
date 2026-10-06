defmodule Slap.Yjs.DocServer.Compaction do
  @moduledoc false
  # A document server's position in the document's stream, and its
  # compactions: what it has read since the current snapshot decides when
  # it compacts, and a compaction, once published or superseded, counts as
  # the current snapshot from then on.

  @type running :: %{
          offset: non_neg_integer(),
          since: non_neg_integer(),
          appends: non_neg_integer(),
          bytes: non_neg_integer(),
          stale: boolean()
        }

  @type t :: %__MODULE__{
          compact_bytes: non_neg_integer(),
          read_offset: non_neg_integer(),
          since_snapshot: non_neg_integer(),
          snapshot_bytes: non_neg_integer(),
          appends_at_snapshot: non_neg_integer(),
          running: running() | nil
        }

  @enforce_keys [:compact_bytes, :read_offset, :since_snapshot, :snapshot_bytes]
  defstruct [
    :compact_bytes,
    # Everything before it has been applied.
    :read_offset,
    # Bytes read since the current snapshot, and its size.
    :since_snapshot,
    :snapshot_bytes,
    # Appends acknowledged when the current snapshot was taken: those since
    # (not read back yet, perhaps) count on stop.
    appends_at_snapshot: 0,
    running: nil
  ]

  @spec new(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: t()
  def new(compact_bytes, offset, since, snapshot_bytes) do
    %__MODULE__{
      compact_bytes: compact_bytes,
      read_offset: offset,
      since_snapshot: since,
      snapshot_bytes: snapshot_bytes
    }
  end

  @spec read(t(), non_neg_integer(), non_neg_integer()) :: t()
  def read(c, offset, bytes),
    do: %{c | read_offset: offset, since_snapshot: c.since_snapshot + bytes}

  @doc """
  The server reloaded the snapshot at `offset`, of `bytes`. A compaction
  running started before it: its counts are not the reloaded snapshot's.
  """
  @spec reloaded(t(), non_neg_integer(), non_neg_integer()) :: t()
  def reloaded(c, offset, bytes) do
    running = c.running && %{c.running | stale: true}
    %{c | read_offset: offset, since_snapshot: 0, snapshot_bytes: bytes, running: running}
  end

  @doc """
  Whether to compact now: none runs, and what was read since the current
  snapshot reaches `max(compact_bytes, snapshot size / 2)`.
  """
  @spec due?(t()) :: boolean()
  def due?(%{running: nil, since_snapshot: since} = c),
    do: since > 0 and since >= c.compact_bytes and since >= div(c.snapshot_bytes, 2)

  def due?(_c), do: false

  @spec behind?(t(), non_neg_integer()) :: boolean()
  def behind?(c, appends), do: c.since_snapshot > 0 or appends > c.appends_at_snapshot

  @spec started(t(), non_neg_integer(), non_neg_integer()) :: t()
  def started(c, bytes, appends) do
    running = %{
      offset: c.read_offset,
      since: c.since_snapshot,
      appends: appends,
      bytes: bytes,
      stale: false
    }

    %{c | running: running}
  end

  @doc """
  The running compaction ended. Superseded (another server compacted
  further) counts as taken: counting goes on from here, as if this one had.
  One that started before a reload changes nothing but that it ended: the
  reload counts from the snapshot it loaded, which is newer or the same.
  """
  @spec finished(t(), :ok | {:error, term()}) :: t()
  def finished(%{running: %{stale: true}} = c, _result), do: %{c | running: nil}

  def finished(%{running: r} = c, result) when result in [:ok, {:error, :superseded}] do
    %{
      c
      | running: nil,
        since_snapshot: c.since_snapshot - r.since,
        snapshot_bytes: r.bytes,
        appends_at_snapshot: r.appends
    }
  end

  def finished(c, {:error, _}), do: %{c | running: nil}
end
