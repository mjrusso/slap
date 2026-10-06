defmodule Slap.Cluster do
  @moduledoc """
  Runs N SlateDB databases ("shards") in one bucket: opens and closes them,
  turns SlateDB's writer fencing into supervised events, fans out durability
  notifications, and finds the shard for a key. It knows nothing about what
  the application stores; the application puts its own per-shard processes
  on top.

  Define one module per cluster. `use` takes `:otp_app` and optionally
  `:defaults`, options that the application environment and `start_link/1`
  can override (for example `shard_children` in a library that builds on
  this one):

      defmodule MyApp.Cluster do
        use Slap.Cluster, otp_app: :my_app
      end

      # config/runtime.exs
      config :my_app, MyApp.Cluster,
        store: {:url, "s3://bucket/prefix", aws_endpoint: "https://rustfs:9000"},
        shards: 64,
        settings: %{flush_interval: "10ms"},
        cache: [capacity_bytes: 2 * 1024 * 1024 * 1024],
        shard_children: {MyApp.ShardChildren, :child_specs, []}

  and add `MyApp.Cluster` to the application's supervision tree. On start
  it checks that the store honours conditional writes (see
  `Slap.SlateDB.probe_store/3`), then the strategy opens the shards. With the
  default `Slap.Cluster.Strategy.Local`, every shard opens on this node
  before `start_link/1` returns.

  ## Options

    * `:store` - required. A store, as for `Slap.SlateDB.open/2`. Shard `n` is the
      database `<path>/shard-00n` in it.
    * `:shards` - required. The number of shards. Fixed for the life of the
      data: `shard_for/1` depends on it.
    * `:path` - the path of the shards in the store (default `""`).
    * `:settings` - SlateDB settings for every shard, as for `Slap.SlateDB.open/2`.
    * `:cache` - `[capacity_bytes: n]` for one block cache shared by this
      node's shards, `:disabled`, or `nil` (default) for SlateDB's own cache
      per shard.
    * `:merge_operator` - as for `Slap.SlateDB.open/2`.
    * `:strategy` - `module` or `{module, opts}` (default
      `Slap.Cluster.Strategy.Local`). See `Slap.Cluster.Strategy`.
    * `:shard_children` - `{mod, fun, args}` or a 1-arity function, called
      with the shard's `Slap.Cluster.Shard` context (prepended to `args`),
      returning child specs to start for each shard. See "Shard children".
    * `:child_options` - application settings in the shard context (default
      `[]`). The cluster does not interpret them. When `:shard_children` is
      `{module, function, args}`, the module may define
      `c:Slap.Cluster.ShardChildren.validate_child_options!/1` to reject invalid
      settings before startup.
    * `:probe` - run the conditional write probe on start (default `true`).
    * `:close_timeout` - ms to wait for a shard's database to close on stop
      (default 30,000).
    * `:lag_interval` - ms between durability lag telemetry samples per
      shard (default 1,000).

  ## Shard children

  When a shard opens on a node, its children start under a supervisor, after
  the database is open. They get a `Slap.Cluster.Shard` context with the
  database handle, and name themselves with `via/2`.

  When the shard stops (the cluster shuts down, the strategy moves it, or
  it is fenced), the children stop first, while the database is still open,
  then the database is closed. Children that need to fail in-flight work
  should trap exits and check `shard_status/1` in `terminate/2`: `:stopping`
  or `:fenced`. This works for processes anywhere under the children, not
  only for the direct ones, which is why it is not the exit reason.

  If the database process crashes, the shard stops, and the strategy
  decides whether to reopen it (`handle_shard_down/3` with `:crashed`).

  ## Durability

  Writes go straight to the context's `db`. `notify_when_durable/3` sends a
  message once a write's `seq` is durable. All notifications for a shard
  come from one process, which releases waiters lowest seq first, so a
  process that asks in seq order (as a single writer does) receives them in
  seq order. One SlateDB subscription per shard serves every waiter.

  ## Telemetry

  Events are sent under `[:slap, :cluster, ...]`: `[:shard, :start | :stop | :fenced]`,
  `[:durability, :lag]` (`%{lag: last_write_seq - durable_seq}`, every
  `:lag_interval`), and `[:probe]`. Metadata includes `:cluster` and
  `:shard`.

  Invalid configuration and call arguments raise. Routing failures return
  `{:error, reason}` from `call/4` and `lookup/2`.
  """

  alias Slap.Cluster.{Config, Hash, Host, Shard, ShardDb, Telemetry}
  alias Slap.SlateDB

  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    defaults = Keyword.get(opts, :defaults, [])

    quote do
      @otp_app unquote(otp_app)
      @defaults unquote(defaults)

      @doc false
      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
      end

      @doc """
      Starts the cluster. `opts` override the application environment, which
      overrides the `:defaults` given to `use Slap.Cluster`.
      """
      def start_link(opts \\ []),
        do: Slap.Cluster.Supervisor.start_link(__MODULE__, @otp_app, opts, @defaults)

      @doc "Stops the cluster on this node, closing its shards."
      def stop(timeout \\ :infinity),
        do: Supervisor.stop(Module.concat(__MODULE__, Supervisor), :normal, timeout)

      @doc "The shard for a placement key. See `Slap.Cluster.shard_for/2`."
      def shard_for(key), do: Slap.Cluster.shard_for(__MODULE__, key)

      @doc "Where a shard is. See `Slap.Cluster.lookup/2`."
      def lookup(shard), do: Slap.Cluster.lookup(__MODULE__, shard)

      @doc "Runs a function with a shard's context. See `Slap.Cluster.call/4`."
      def call(shard, mfa, opts \\ []), do: Slap.Cluster.call(__MODULE__, shard, mfa, opts)

      @doc "A name for a per-shard process. See `Slap.Cluster.via/3`."
      def via(shard, name), do: Slap.Cluster.via(__MODULE__, shard, name)

      @doc "The shards open on this node."
      def local_shards, do: Slap.Cluster.local_shards(__MODULE__)

      @doc "Where every shard is. See `Slap.Cluster.assignments/1`."
      def assignments, do: Slap.Cluster.assignments(__MODULE__)
    end
  end

  @doc """
  The shard for `key`: `xxh64(key) mod shards`. The hash is pinned; see
  `Slap.Cluster.Hash`.
  """
  @spec shard_for(module(), binary()) :: non_neg_integer()
  def shard_for(cluster, key) when is_binary(key),
    do: Hash.shard_for(key, Config.get(cluster).shards)

  def shard_for(_cluster, _key), do: raise(ArgumentError, "key must be a binary")

  @doc """
  Where `shard` is: `{:ok, {:local, ctx}}` with its context when it is open
  on this node, `{:ok, {:remote, node}}` when the strategy places it on
  another node, or `{:error, :unassigned}`.
  """
  @spec lookup(module(), non_neg_integer()) ::
          {:ok, {:local, Shard.t()} | {:remote, node()}} | {:error, :unassigned}
  def lookup(cluster, shard) do
    %Config{shards: shards, strategy: {strategy, _}} = Config.get(cluster)

    unless is_integer(shard) and shard >= 0 and shard < shards,
      do: raise(ArgumentError, "shard must be in 0..#{shards - 1}, got: #{inspect(shard)}")

    case Registry.lookup(registry(cluster), {:shard, shard}) do
      [{_pid, %Shard{status: status} = ctx}] ->
        if Shard.status(status) == :running, do: {:ok, {:local, ctx}}, else: {:error, :unassigned}

      [] ->
        case strategy.lookup(cluster, shard) do
          {:ok, {:remote, node}} -> {:ok, {:remote, node}}
          _ -> {:error, :unassigned}
        end
    end
  end

  @call_timeout 30_000

  @doc """
  Runs `mod.fun(ctx, ...args)` with the context of `shard` on the node that
  owns it: here when the shard is open on this node, otherwise over `:erpc`
  on the owner (multi-node strategies need distributed Erlang). Returns
  `{:ok, result}` for the application's result, or a routing error:

    * `{:error, :unassigned}` - no node owns the shard (for example during
      a failover), after the retries.
    * `{:error, :not_owner}` - the owner the strategy named no longer has
      the shard, after the retries.
    * `{:error, {:erpc, reason}}` - the owner could not be reached
      (`:noconnection`) or the call timed out (`:timeout`).

  On `:not_owner`, `:unassigned`, or an owner that cannot be connected to
  (the call was not sent), the strategy's view is
  refreshed and the call retried, up to `:retries` times (default 3), after
  a jittered delay of about `:retry_delay` ms (default 100) times the
  attempt. `:timeout` (ms, default 30,000; `:infinity` for none) bounds
  the remote route, including retries and connecting. A local `mod.fun`
  runs in the caller and must enforce its own timeout. `mod` must be
  loaded on the owner. Connecting to an owner that is not connected yet
  waits at most a second.

  Telemetry: `[:slap, :cluster, :call, :remote]` for each remote call and
  `[:slap, :cluster, :call, :not_owner]` for each stale owner.
  """
  @spec call(module(), non_neg_integer(), {module(), atom(), list()}, keyword()) ::
          {:ok, term()} | {:error, :unassigned | :not_owner | {:erpc, term()}}
  def call(cluster, shard, mfa, opts \\ [])

  def call(cluster, shard, {mod, fun, args} = mfa, opts)
      when is_atom(mod) and is_atom(fun) and is_list(args) and is_list(opts) do
    unless is_atom(cluster) and cluster != nil,
      do: raise(ArgumentError, "cluster must be a module")

    unless Keyword.keyword?(opts),
      do: raise(ArgumentError, "options must be a keyword list")

    Keyword.validate!(opts, [:timeout, :retries, :retry_delay])
    validate_call_opts!(opts)
    timeout = Keyword.get(opts, :timeout, @call_timeout)
    do_call(cluster, shard, mfa, opts, deadline(timeout), Keyword.get(opts, :retries, 3), 1)
  end

  def call(_cluster, _shard, {mod, fun, args}, _opts)
      when is_atom(mod) and is_atom(fun) and is_list(args),
      do: raise(ArgumentError, "options must be a keyword list")

  def call(_cluster, _shard, _mfa, _opts),
    do: raise(ArgumentError, "mfa must be {module, function, args} with a list of args")

  defp validate_call_opts!(opts) do
    if Keyword.has_key?(opts, :timeout) and
         not (opts[:timeout] == :infinity or
                (is_integer(opts[:timeout]) and opts[:timeout] >= 0)),
       do: raise(ArgumentError, ":timeout must be a non-negative integer or :infinity")

    for key <- [:retries, :retry_delay], Keyword.has_key?(opts, key) do
      value = opts[key]

      unless is_integer(value) and value >= 0,
        do: raise(ArgumentError, "#{inspect(key)} must be a non-negative integer")
    end
  end

  defp do_call(cluster, shard, mfa, opts, deadline, retries, attempt) do
    if remaining(deadline) == 0,
      do: {:error, {:erpc, :timeout}},
      else: call_before_deadline(cluster, shard, mfa, opts, deadline, retries, attempt)
  end

  defp call_before_deadline(
         cluster,
         shard,
         {mod, fun, args} = mfa,
         opts,
         deadline,
         retries,
         attempt
       ) do
    result =
      case lookup(cluster, shard) do
        {:ok, {:local, ctx}} ->
          {:done, {:ok, apply(mod, fun, [ctx | args])}}

        {:ok, {:remote, node}} ->
          Telemetry.execute([:call, :remote], %{}, %{cluster: cluster, shard: shard, node: node})
          remote(cluster, shard, node, mfa, deadline)

        {:error, :unassigned} ->
          {:retry, {:error, :unassigned}}
      end

    case result do
      {:done, value} ->
        value

      {:retry, _error} when retries > 0 ->
        refresh(cluster, deadline)
        delay = Keyword.get(opts, :retry_delay, 100) * attempt
        sleep_before_retry(deadline, delay + :rand.uniform(max(div(delay, 2), 1)))
        do_call(cluster, shard, mfa, opts, deadline, retries - 1, attempt + 1)

      {:retry, error} ->
        error
    end
  end

  # Only a call that was never sent is retried: once sent, `mfa` may have
  # run on the owner even if the connection then failed, and running it
  # again (an append) would not be safe.
  defp remote(cluster, shard, node, mfa, deadline) do
    if connected?(node, remaining(deadline)) do
      case :erpc.call(node, __MODULE__, :call_owner, [cluster, shard, mfa], remaining(deadline)) do
        {:error, :not_owner} = error ->
          Telemetry.execute([:call, :not_owner], %{}, %{
            cluster: cluster,
            shard: shard,
            node: node
          })

          {:retry, error}

        {:ok, value} ->
          {:done, {:ok, value}}
      end
    else
      {:retry, {:error, {:erpc, :noconnection}}}
    end
  catch
    :error, {:erpc, reason} -> {:done, {:error, {:erpc, reason}}}
  end

  defp connected?(node, timeout), do: node in Node.list() or connect(node, timeout)

  # Connecting can block for up to net_setuptime (7 s) on an unreachable
  # host: wait a second at most (the attempt goes on in the background).
  defp connect(node, timeout) do
    task = Task.async(fn -> Node.connect(node) end)

    Task.yield(task, min_timeout(timeout, 1_000)) == {:ok, true} or
      (Task.shutdown(task, :brutal_kill) && false)
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(ms) when is_integer(ms) and ms >= 0, do: System.monotonic_time(:millisecond) + ms

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp min_timeout(:infinity, cap), do: cap
  defp min_timeout(timeout, cap), do: min(timeout, cap)

  defp sleep_before_retry(:infinity, ms), do: Process.sleep(ms)
  defp sleep_before_retry(deadline, ms), do: Process.sleep(min(ms, remaining(deadline)))

  @doc false
  # On the owner: the outer tuple separates routing from the application's result.
  def call_owner(cluster, shard, {mod, fun, args}) do
    case local_shard(cluster, shard) do
      [{_pid, %Shard{status: status} = ctx}] ->
        if Shard.status(status) == :running,
          do: {:ok, apply(mod, fun, [ctx | args])},
          else: {:error, :not_owner}

      [] ->
        {:error, :not_owner}
    end
  end

  # The cluster may not be running on this node at all.
  defp local_shard(cluster, shard) do
    Registry.lookup(registry(cluster), {:shard, shard})
  rescue
    ArgumentError -> []
  end

  # Asks the strategy to refresh its view of the owners, if it keeps one.
  defp refresh(cluster, deadline) do
    %Config{strategy: {strategy, _}} = Config.get(cluster)

    if function_exported?(strategy, :refresh, 2),
      do: strategy.refresh(cluster, min_timeout(remaining(deadline), 5_000))

    :ok
  end

  @doc """
  A `:via` name for a per-shard process, registered in the cluster's
  registry on the node that owns the shard.
  """
  @spec via(module(), non_neg_integer(), term()) :: {:via, Registry, {atom(), term()}}
  def via(cluster, shard, name), do: {:via, Registry, {registry(cluster), {shard, name}}}

  @doc "The shards open on this node, in order."
  @spec local_shards(module()) :: [non_neg_integer()]
  def local_shards(cluster), do: Host.local_shards(cluster)

  @doc """
  Every shard's location as this node sees it: `{:local, pid}` (the shard's
  database process), `{:remote, node}` or `:unassigned`.
  """
  @spec assignments(module()) :: %{non_neg_integer() => term()}
  def assignments(cluster) do
    for n <- 0..(Config.get(cluster).shards - 1), into: %{} do
      case lookup(cluster, n) do
        {:ok, {:local, ctx}} -> {n, {:local, ctx.shard_db}}
        {:ok, remote} -> {n, remote}
        {:error, :unassigned} -> {n, :unassigned}
      end
    end
  end

  @doc """
  Sends `{:slap_cluster_durable, tag}` to the calling process (or
  `:to`) once the shard's durable sequence number reaches `seq`, the value
  a write returned. If it already has, the message is sent at once.

  A process that asks in seq order gets the notifications in seq order, and
  waiters that become durable together are released lowest seq first.
  Notifications are dropped if the shard stops first; check
  `shard_status/1` then.
  """
  @spec notify_when_durable(Shard.t(), non_neg_integer(), term(), keyword()) :: :ok
  def notify_when_durable(%Shard{} = ctx, seq, tag, opts \\ [])
      when is_integer(seq) and seq >= 0 do
    ShardDb.notify_when_durable(ctx, seq, tag, Keyword.get(opts, :to, self()))
  end

  @doc "The shard's durable sequence number. See `Slap.SlateDB.durable_seq/1`."
  @spec durable_seq(Shard.t()) :: non_neg_integer()
  def durable_seq(%Shard{db: db}), do: SlateDB.durable_seq(db)

  @doc """
  `:running`, `:stopping` (the shard is being closed or moved) or `:fenced`
  (another writer opened its database).
  """
  @spec shard_status(Shard.t()) :: :running | :stopping | :fenced
  def shard_status(%Shard{status: status}), do: Shard.status(status)

  @doc false
  def registry(cluster), do: Module.concat(cluster, Registry)
  @doc false
  def host(cluster), do: Module.concat(cluster, Host)
  @doc false
  def shard_supervisors(cluster), do: Module.concat(cluster, ShardSupervisors)
  @doc false
  # The dynamic supervisor of shard `n` (a partition of shard_supervisors/1).
  def shard_supervisor(cluster, n),
    do: {:via, PartitionSupervisor, {shard_supervisors(cluster), n}}
end
