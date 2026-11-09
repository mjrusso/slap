defmodule Slap.Inspect.Records do
  @moduledoc false

  alias Slap.Inspect.{Content, Home, Log, Operation, Remote}

  @streams Slap.Streams.Introspect

  def page(connection, %{operation: :home}, deadline), do: Home.page(connection, deadline)

  def page(connection, %{operation: :logical} = request, deadline),
    do: logical(connection, request, deadline)

  def page(connection, %{operation: :retention_metadata} = request, deadline),
    do: retention_metadata(connection, request, deadline)

  def page(connection, %{operation: :log_catalog} = request, deadline),
    do: log_catalog(connection, request, deadline)

  def page(connection, %{operation: :log_component} = request, deadline),
    do: log_component(connection, request, deadline)

  def page(connection, %{operation: :yjs_log} = request, deadline),
    do: yjs_log(connection, request, deadline)

  def page(connection, request, deadline) do
    if Operation.manual?(request.operation),
      do: Content.execute(connection, request, deadline),
      else: target(connection, request, deadline)
  end

  defp target(connection, request, deadline),
    do: Remote.read(connection, request, Operation.target(request.operation), deadline)

  defp retention_metadata(connection, request, deadline) do
    with {:ok, stream} <- Remote.read(connection, request, {@streams, :stream}, deadline),
         {:ok, page} <- target(connection, request, deadline) do
      source =
        with {:ok, meta} <- metadata(stream.row),
             :ok <- identity(meta, request.opts[:sid]),
             do: {:ok, stream}

      {:ok, Map.put(page, :metadata, source)}
    end
  end

  defp log_catalog(connection, request, deadline) do
    with {:ok, page} <- target(connection, request, deadline),
         {:ok, log} <- describe(connection, request, page, deadline),
         do: {:ok, Map.put(page, :log, log)}
  end

  defp describe(connection, request, page, deadline) do
    log = Log.describe(connection, request, page, deadline)

    if page.rows == [] and Enum.all?(Map.values(log.components), &is_nil/1),
      do: {:error, {:log_not_found, request.shard}},
      else: {:ok, log}
  end

  defp log_component(connection, request, deadline) do
    component = request.opts[:component]

    with {:ok, paths} <- paths(connection, request, deadline) do
      logical(
        connection,
        %{request | opts: Keyword.put(request.opts, :path, Map.fetch!(paths, component))},
        deadline,
        []
      )
    end
  end

  defp yjs_log(connection, request, deadline) do
    catalog = %{request | opts: Keyword.drop(request.opts, [:cursor, :cursor_exclusive])}

    with {:ok, page} <-
           Remote.read(connection, catalog, {Slap.SnapshotLog.Introspect, :scan}, deadline),
         {:ok, log} <- describe(connection, catalog, page, deadline),
         %{decoded: {:ok, %{path: path, value: %{sid: sid}}}} <- log.components.updates do
      opts =
        Keyword.merge(request.opts, path: path, expected_sid: sid)

      request = %{request | opts: opts}

      with {:ok, page} <- logical(connection, request, deadline, []),
           do: {:ok, Map.put(page, :log, log)}
    else
      {:error, _} = error -> error
      _ -> {:error, :updates_not_observed}
    end
  end

  def shard(connection, request) do
    case {Operation.route(request.operation), request.shard} do
      # Read by the entry node; no shard owns it.
      {:entry, _shard} -> {:ok, nil}
      {_route, shard} when is_integer(shard) -> {:ok, shard}
      {:partition, nil} -> route_key(connection, request.opts[:partition])
      {:stream, nil} -> stream_route(connection, request.opts)
    end
  end

  defp stream_route(connection, opts) do
    case {opts[:placement_key], opts[:base] || opts[:path]} do
      {key, _path} when is_binary(key) -> route_key(connection, key)
      {nil, nil} -> default_shard()
      {nil, path} -> stream_shard(connection, path)
    end
  end

  defp route_key(_connection, nil), do: default_shard()

  defp route_key(connection, key),
    do: Remote.call(connection.entry, Slap.Cluster, :shard_for, [connection.cluster, key])

  defp stream_shard(connection, path) do
    case Remote.call(connection.entry, Slap.Streams.Introspect, :shard_for, [
           connection.cluster,
           Remote.api_version(),
           path
         ]) do
      {:ok, shard} when is_integer(shard) -> {:ok, shard}
      {:ok, {:error, _} = error} -> error
      error -> error
    end
  end

  # Every cluster has shard 0; reading it reports an unassigned shard.
  defp default_shard, do: {:ok, 0}

  defp logical(connection, request, deadline), do: logical(connection, request, deadline, [])

  defp logical(connection, request, deadline, seen) do
    identity = {request.shard, request.opts[:path]}

    if identity in seen,
      do: {:error, :lineage_cycle},
      else: logical_stream(connection, request, deadline, [identity | seen])
  end

  defp logical_stream(connection, request, deadline, seen) do
    with {:ok, stream} <- Remote.read(connection, request, {@streams, :logical}, deadline),
         {:ok, page} <- logical_page(connection, request, stream.logical, deadline, seen) do
      {:ok,
       Map.merge(page, %{
         metadata: stream,
         lineage: page[:lineage] || [],
         interpretation: :logical,
         soft_deleted: stream.logical.soft_deleted,
         creation: stream.logical.creation
       })}
    end
  end

  defp metadata(%{decoded: {:ok, %{value: %{sid: _} = meta}}}), do: {:ok, meta}
  defp metadata(nil), do: {:error, :not_found_custom_placement_key_possible}
  defp metadata(_row), do: {:error, :unreadable_metadata}
  defp identity(_meta, nil), do: :ok
  defp identity(%{sid: sid}, sid), do: :ok
  defp identity(_meta, _expected), do: {:error, :stream_identity_changed}

  defp logical_page(
         connection,
         request,
         %{read_plan: %{kind: :source} = plan} = meta,
         deadline,
         seen
       ) do
    source = apply_plan(request, plan)

    with {:ok, page} <- logical(connection, source, deadline, seen) do
      cursor = page.cursor || meta.continuation
      lineage = [%{capture: meta.reference, metadata: page.metadata} | page.lineage]
      {:ok, %{page | cursor: cursor, cursor_exclusive: false, lineage: lineage}}
    end
  end

  defp logical_page(
         connection,
         request,
         %{read_plan: %{kind: :messages} = plan},
         deadline,
         _seen
       ),
       do: target(connection, apply_plan(request, plan), deadline)

  defp logical_page(_connection, _request, _meta, _deadline, _seen),
    do: {:error, :unsupported_logical_plan}

  defp apply_plan(request, plan), do: %{request | shard: plan.shard, opts: plan.opts}

  defp paths(connection, request, deadline) do
    with {:ok, budget} <- Remote.remaining(deadline),
         {:ok, result} <-
           Remote.call(
             connection.entry,
             Slap.SnapshotLog.Introspect,
             :paths,
             [Remote.api_version(), Keyword.fetch!(request.opts, :base)],
             min(budget, 5000)
           ),
         do: result
  end
end
