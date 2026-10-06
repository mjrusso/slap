defmodule Slap.Yjs.Follower do
  @moduledoc false

  alias Slap.SnapshotLog
  alias Slap.Yjs
  alias Slap.Yjs.Store

  @backoff_min 50
  @backoff_max 2_000

  @doc "Errors worth retrying."
  defguard transient?(reason) when reason in [:unavailable, :timeout, :overloaded]

  @doc "Starts following `doc` from `offset` for `server`, linked to the caller."
  @spec start_link(Store.doc(), non_neg_integer(), pid(), keyword()) :: pid()
  def start_link(doc, offset, server \\ self(), opts \\ []) do
    base = Store.base(doc, opts)

    spawn_link(fn ->
      follow(
        %{
          base: base,
          cluster: Keyword.take(opts, [:cluster]),
          server: server,
          backoff: @backoff_min,
          unacked: false
        },
        offset
      )
    end)
  end

  @doc "Acknowledges a message from `follower`, once the server has applied it."
  @spec ack(pid()) :: :ok
  def ack(follower) do
    send(follower, {__MODULE__, :ack})
    :ok
  end

  defp follow(s, offset) do
    case SnapshotLog.next(s.base, offset, s.cluster) do
      # Waited at the tail, and nothing came.
      {:ok, %{entries: [], offset: next}} ->
        follow(%{s | backoff: @backoff_min}, next)

      {:ok, %{entries: entries, offset: next}} ->
        updates = Enum.flat_map(entries, &parse/1)
        bytes = Enum.sum_by(entries, &byte_size/1)
        s |> deliver({:updates, updates, next, bytes}) |> follow(next)

      {:reset, %{offset: next} = reset} ->
        s |> deliver({:reload, reset}) |> follow(next)

      {:error, :deleted} ->
        exit({:shutdown, :deleted})

      {:error, reason} when transient?(reason) ->
        Process.sleep(s.backoff)
        follow(%{s | backoff: min(s.backoff * 2, @backoff_max)}, offset)

      {:error, reason} ->
        exit({:slap_yjs_follow_failed, reason})
    end
  end

  # The follower is linked to the server, so it does not outlive a server
  # that will not acknowledge.
  defp deliver(s, message) do
    if s.unacked do
      receive do
        {__MODULE__, :ack} -> :ok
      end
    end

    send(s.server, {__MODULE__, self(), message})
    %{s | backoff: @backoff_min, unacked: true}
  end

  defp parse(entry) do
    case Yjs.Frame.parse(entry) do
      {:ok, updates} -> updates
      {:error, reason} -> exit({:slap_yjs_follow_failed, {:bad_frames, reason}})
    end
  end
end
