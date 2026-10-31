defmodule Slap.SnapshotLog.Control do
  @moduledoc false

  alias Slap.SnapshotLog.Store
  alias Slap.Streams

  def path(store), do: Store.path(store, :control)

  def deadline(:infinity), do: :infinity
  def deadline(timeout), do: System.monotonic_time(:millisecond) + timeout
  def remaining(:infinity), do: :infinity
  def remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  def opts(store, deadline),
    do: Keyword.put(Store.stream_opts(store), :timeout, remaining(deadline))

  def read(store, deadline), do: read(store, :start, deadline)

  defp read(store, from, deadline) do
    case Streams.read_fenced(
           path(store),
           from,
           [max_bytes: 1024 * 1024] ++ opts(store, deadline)
         ) do
      {:ok, %{up_to_date: false, next_offset: next}} ->
        read(store, next, deadline)

      {:ok, %{messages: messages, next_offset: offset} = view} ->
        {_at, body} = List.last(messages)
        {:ok, control} = parse(body)

        {:ok, Map.merge(control, %{identity: identity(view, path(store), store), offset: offset})}

      {:error, :not_found} ->
        {:ok, nil}

      error ->
        error
    end
  end

  @states %{
    {"ordinary", "ready"} => :ordinary,
    {"ordinary", "cancelled"} => :ordinary_deleted,
    {"initialization", "pending"} => :initializing,
    {"initialization", "cancelled"} => :initialization_cancelled,
    {"fork", "pending"} => :forking,
    {"fork", "ready"} => :forked,
    {"fork", "cancelled"} => :fork_cancelled
  }

  @doc "A control record and its state, or `:error` if `json` is not one."
  def parse(json) do
    with {:ok, %{"kind" => kind, "status" => status} = record} <- JSON.decode(json),
         {:ok, state} <- Map.fetch(@states, {kind, status}) do
      {:ok, %{record: record, state: state}}
    else
      _ -> :error
    end
  end

  def check(store, deadline) do
    with {:ok, control} <- read(store, deadline), do: access(control)
  end

  def access(nil), do: :ok
  def access(%{state: state}) when state in [:ordinary, :forked], do: :ok
  def access(%{state: state}) when state in [:initializing, :forking], do: {:error, :busy}

  def access(%{state: state})
      when state in [:ordinary_deleted, :initialization_cancelled, :fork_cancelled],
      do: {:error, :deleted}

  def claim(store, record, absent, deadline) do
    work = record["status"] == "pending" or record["cleanup"] == true

    case Streams.control_create(
           path(store),
           JSON.encode!(record),
           absent,
           work,
           opts(store, deadline)
         ) do
      {:ok, status, _} ->
        with {:ok, control} <- read(store, deadline), do: {:ok, status, control}

      error ->
        error
    end
  end

  def put(store, control, record, deadline, unsealed \\ true) do
    work = record["status"] == "pending" or record["cleanup"] == true

    case Streams.control_append(
           path(store),
           control.identity,
           control.offset,
           JSON.encode!(record),
           work,
           unsealed,
           opts(store, deadline)
         ) do
      {:ok, _} -> read(store, deadline)
      error -> error
    end
  end

  def guard(control), do: %{identity: control.identity, offset: control.offset}

  def finish_cleanup(store, control, deadline) do
    case put(store, control, Map.put(control.record, "cleanup", false), deadline, false) do
      {:ok, _} -> :ok
      {:error, :conflict} -> completed_cleanup(store, control.identity, deadline)
      error -> error
    end
  end

  defp completed_cleanup(store, identity, deadline) do
    case read(store, deadline) do
      {:ok, %{identity: ^identity, record: %{"status" => "cancelled", "cleanup" => false}}} -> :ok
      {:ok, _newer_work} -> {:error, :conflict}
      error -> error
    end
  end

  def remove_component(store, path, owner, deadline) do
    with {:ok, identity} <-
           Streams.internal(path, {:owned_identity, owner}, opts(store, deadline)) do
      if identity, do: Streams.delete_identity(identity, opts(store, deadline)), else: :ok
    end
  end

  def identity(view, path, store) do
    cluster = store.cluster || Slap.Streams.Cluster
    %{path: path, shard: Slap.Cluster.shard_for(cluster, store.base), sid: view.sid}
  end

  def encode_identity(%{path: path, shard: shard, sid: sid}),
    do: %{"path" => path, "shard" => shard, "sid" => sid}

  def decode_identity(%{"path" => path, "shard" => shard, "sid" => sid}),
    do: %{path: path, shard: shard, sid: sid}
end
