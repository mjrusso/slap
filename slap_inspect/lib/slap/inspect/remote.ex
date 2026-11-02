defmodule Slap.Inspect.Remote do
  @moduledoc false

  alias Slap.Inspect.Operation
  alias Slap.Inspect.Remote.Error

  @cluster Slap.Cluster
  # Sent with every versioned call; the server checks it (Slap.Cluster.Inspection).
  @api_version 1
  @allowed [
    {@cluster, :clusters, 0},
    {@cluster, :status, 2},
    {@cluster, :assignments, 1},
    {@cluster, :shard_for, 2},
    {Slap.Streams.Introspect, :shard_for, 3},
    {Slap.Yjs.Introspect, :limits, 0},
    {Slap.Yjs.Introspect, :reconstruct, 3},
    {Slap.SnapshotLog.Introspect, :paths, 2}
  ]
  # Calls routed to shard owners: every operation's target, and the reads
  # that logical views and content collection run.
  @reads Operation.targets() ++
           [{Slap.Streams.Introspect, :logical}, {Slap.SnapshotLog.Introspect, :capture}]

  def api_version, do: @api_version

  def connect(entry, cookie_file, opts) do
    with {:ok, raw} <- File.read(cookie_file),
         cookie = String.trim(raw),
         true <- byte_size(cookie) in 1..255 and String.valid?(cookie),
         :ok <- start_node(opts) do
      Node.set_cookie(String.to_atom(cookie))
      if :net_kernel.hidden_connect_node(entry), do: :ok, else: {:error, :connection_failed}
    else
      _ -> {:error, :connection_failed}
    end
  rescue
    _error in [ArgumentError, SystemLimitError] -> {:error, :connection_failed}
  end

  defp start_node(_opts) when node() != :nonode@nohost, do: :ok

  defp start_node(opts) do
    mode = Keyword.fetch!(opts, :name_domain)
    name = "slap_inspect_#{System.pid()}_#{System.system_time(:nanosecond)}"
    name = if mode == :longnames, do: name <> "@127.0.0.1", else: name

    with {:ok, _pid} <-
           :net_kernel.start(String.to_atom(name), %{
             name_domain: mode,
             hidden: true,
             dist_listen: false
           }),
         do: :ok
  end

  def call(node, module, function, args, timeout \\ 5000) do
    if {module, function, length(args)} in @allowed,
      do:
        protect(node, {module, function, length(args)}, fn ->
          connected_call(node, module, function, args, timeout)
        end),
      else: {:error, :forbidden_target}
  end

  defp connected_call(node, module, function, args, timeout) do
    with :ok <- owner_connection(node),
         do: {:ok, :erpc.call(node, module, function, args, timeout)}
  end

  @doc """
  Calls `module.function(cluster, shard, version, budget, opts)` on the owner
  of `request.shard`. Routing and every owner request, including retries, get
  only the time left before `deadline`; none starts after it.
  """
  def read(connection, request, {module, function} = target, deadline) do
    if target in @reads do
      route(%{
        entry: connection.entry,
        cluster: connection.cluster,
        shard: request.shard,
        module: module,
        function: function,
        deadline: deadline,
        opts: request.opts,
        observer: Map.get(request, :observer),
        retries: 2
      })
    else
      {:error, :forbidden_target}
    end
  end

  defp route(request) do
    with {:ok, timeout} <- remaining(request.deadline),
         {:ok, owner} <- owner(request, timeout),
         :ok <- owner_connection(owner),
         {:ok, budget} <- remaining(request.deadline) do
      result =
        protect(owner, {request.module, request.function, 5}, fn ->
          owner_read(owner, request, budget)
        end)

      retry(result, request)
    end
  end

  def remaining(deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      left when left > 0 -> {:ok, left}
      _spent -> {:error, :deadline}
    end
  end

  defp owner_read(owner, request, budget) do
    if request.observer, do: send(request.observer, {:inspection_owner, owner, request.shard})
    # Only an owner reply confirms that native calls have finished.
    args = [request.cluster, request.shard, @api_version, budget, request.opts]
    id = :erpc.send_request(owner, request.module, request.function, args)

    :erpc.receive_response(id, :infinity)
  end

  defp retry({:error, :not_owner}, %{retries: retries} = request) when retries > 0,
    do: route(%{request | retries: retries - 1})

  defp retry(result, _request), do: result

  defp owner(%{shard: nil, entry: entry}, _timeout), do: {:ok, entry}

  defp owner(request, timeout) do
    with {:ok, assignments} <-
           call(request.entry, @cluster, :assignments, [request.cluster], timeout),
         do: owner(assignments, request.shard, request.entry)
  end

  defp owner(assignments, shard, entry) do
    case Map.get(assignments, shard, :unassigned) do
      {:local, _pid} -> {:ok, entry}
      {:remote, owner} -> {:ok, owner}
      :unassigned -> {:error, :unassigned}
    end
  end

  defp owner_connection(owner) do
    if owner == node() or :net_kernel.hidden_connect_node(owner),
      do: :ok,
      else: {:error, {:disconnected, owner}}
  end

  defp protect(node, endpoint, fun) do
    fun.()
  catch
    kind, reason -> failure(%{node: node, endpoint: endpoint}, kind, reason, __STACKTRACE__)
  end

  defp failure(_context, :error, {:erpc, :noconnection}, _stack), do: {:error, :disconnected}
  defp failure(_context, :error, {:erpc, :timeout}, _stack), do: {:error, :timeout}

  defp failure(context, :error, {:exception, reason, stack}, _local_stack),
    do: application_error(context, reason, stack)

  defp failure(context, :exit, {:exception, reason}, _stack),
    do: Error.result(context, :exit, reason, [])

  defp failure(context, :exit, {:signal, _reason} = signal, _stack),
    do: Error.result(context, :exit, signal, [])

  defp failure(context, :throw, reason, _stack), do: Error.result(context, :throw, reason, [])

  defp failure(context, :error, reason, stack), do: application_error(context, reason, stack)
  defp failure(context, kind, reason, stack), do: Error.result(context, kind, reason, stack)

  defp application_error(context, reason, stack) do
    if missing_endpoint?(context, reason, stack),
      do: {:error, :owner_does_not_support_inspection},
      else: Error.result(context, :error, reason, stack)
  end

  defp missing_endpoint?(
         %{node: node, endpoint: {module, function, arity}},
         reason,
         [{module, function, args, _location} | _]
       ) do
    undefined?(reason) and stack_arity(args) == arity and
      missing_export?(node, module, function, arity)
  end

  defp missing_endpoint?(_context, _reason, _stack), do: false
  defp undefined?(:undef), do: true
  defp undefined?(%UndefinedFunctionError{}), do: true
  defp undefined?(_reason), do: false
  defp stack_arity(args) when is_list(args), do: length(args)
  defp stack_arity(arity), do: arity

  defp missing_export?(node, module, function, arity) do
    :erpc.call(node, :erlang, :function_exported, [module, function, arity], 1000) == false
  catch
    _kind, _reason -> false
  end
end
