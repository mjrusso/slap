defmodule Slap.Cluster.Strategy.ObjectLease do
  @moduledoc """
  Places shards on the nodes of a cluster through leases kept as objects in
  the cluster's own object store: no database is needed, only the bucket
  the shards already live in. It is the lease strategy for deployments
  without a database.

  One object per shard, `<path>/_cluster/placement/shards/NNN`, holds
  `{"owner", "expires_at", "generation"}`, and one per node,
  `.../nodes/<name>`, its heartbeat. Claims and renewals are conditional
  writes: create-if-absent for a lease that does not exist yet, and
  `If-Match` on the version last read or written otherwise, so two nodes
  can never both claim or renew the same lease. The loop is
  `Slap.Cluster.Strategy.LeaseLoop`.

  Routing (`Slap.Cluster.call/4`) still goes over distributed Erlang, to
  the node a lease names. That name must already be known on the caller's
  node (an atom: a node it is or was connected to, or one named in its
  configuration), since lease data does not create atoms; so the nodes
  need peer discovery, such as libcluster. Until then, a node's shards are
  unassigned from the nodes that do not know it.

  ## Requirements

    * **A store with conditional updates** (`If-Match`): S3 and S3-compatible
      stores such as RustFS, Azure, GCS. The local file system only has
      create-if-absent, so it cannot be used (cluster startup fails);
      one node on a local directory should use `Local`.
    * **Clocks within about `:max_clock_skew_ms`** (default 1 s): expiry times are
      wall-clock times written by the owner and read by the others. A node
      only treats a lease as expired `:max_clock_skew_ms` after its expiry time.
      Beyond that, a claim can come early, which costs a fence in SlateDB,
      not data.

  ## Cost

  Every interval (a third of the TTL, 5 s by default) each node writes its
  heartbeat, lists and reads the heartbeats, renews its leases (one
  conditional PUT each), and reads every shard's lease: with 64 shards and 3
  nodes, about 20 requests a second per node. A longer TTL costs less and
  fails over more slowly.

  ## Options

    * `:store` - where the lease objects live (default the cluster's store).
    * `:path` - their path in it (default `<cluster path>/_cluster/placement`).
    * `:max_clock_skew_ms` - ms (default 1,000); see above.
    * the loop's options: `:lease_ttl` (default 15 s), `:interval`,
      `:margin`, `:name`, `:cluster_name`, `:max_concurrency`; see
      `Slap.Cluster.Strategy.LeaseLoop`.
  """

  use Slap.Cluster.Strategy.LeaseLoop

  alias Slap.Cluster.Config
  alias Slap.Cluster.Strategy, as: StrategyOptions
  alias Slap.Cluster.Strategy.LeaseLoop
  alias Slap.SlateDB.ObjectStore

  @impl Slap.Cluster.Strategy
  def validate_options(opts) do
    StrategyOptions.validate_options!(
      opts,
      [
        :store,
        :path,
        :max_clock_skew_ms,
        :lease_ttl,
        :interval,
        :margin,
        :name,
        :cluster_name,
        :max_concurrency
      ],
      [:lease_ttl, :interval, :margin, :max_concurrency],
      [:max_clock_skew_ms]
    )
  end

  # -- Lease store ------------------------------------------------------------

  @impl LeaseLoop.Store
  def init(opts, ctx) do
    Map.merge(ctx, %{
      store: Keyword.get_lazy(opts, :store, fn -> Config.get(ctx.cluster).store end),
      path: Keyword.get_lazy(opts, :path, fn -> default_path(Config.get(ctx.cluster)) end),
      skew: Keyword.get(opts, :max_clock_skew_ms, 1_000),
      os: nil,
      # shard => {version, generation} of the leases this node holds.
      held: %{},
      # shard => {lease | nil | :invalid, version}, from the last read of
      # every lease.
      scan: %{}
    })
  end

  defp default_path(config),
    do: Enum.join(Enum.reject([config.path, "_cluster", "placement"], &(&1 == "")), "/")

  # Opens the store, and checks it can do conditional updates.
  @impl LeaseLoop.Store
  def setup(s) do
    with {:ok, os} <- ObjectStore.open(s.path, store: s.store, timeout: s.timeout),
         :ok <- check_updates(os, s),
         s = %{s | os: os},
         :ok <- free_stale(s) do
      {:ok, s}
    else
      {:error, reason} -> {{:error, reason}, s}
    end
  end

  # This node holds nothing yet: leases under its name are left from a
  # previous run that did not stop cleanly. Free them, so they are claimed
  # now rather than unavailable until they expire. Best effort: a lease it
  # cannot free still expires.
  defp free_stale(s) do
    0..(s.shards - 1)
    |> Task.async_stream(
      fn n ->
        with {:ok, {body, version}} <- ObjectStore.get(s.os, key(n), timeout: s.timeout),
             %{owner: owner, generation: g} when owner == s.me <- parse(body) do
          put(s, n, lease(nil, 0, g), {:update, version})
        end
      end,
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Stream.run()
  end

  defp check_updates(os, s) do
    key = "check/#{s.me}"

    with {:ok, v1} <- ObjectStore.put(os, key, "1", timeout: s.timeout),
         {:ok, _v2} <- ObjectStore.put(os, key, "2", mode: {:update, v1}, timeout: s.timeout),
         # An update on a stale version must be refused.
         {:error, :conflict} <-
           ObjectStore.put(os, key, "3", mode: {:update, v1}, timeout: s.timeout) do
      :ok
    else
      {:ok, _} ->
        {:error,
         {:unsupported,
          "#{inspect(s.store)} accepted a conditional update on a stale version; " <>
            "ObjectLease needs If-Match to be enforced"}}

      {:error, :unsupported} ->
        {:error,
         {:unsupported,
          "#{inspect(s.store)} has no conditional updates (If-Match), which ObjectLease needs; " <>
            "use an object store, or Local for one node"}}

      {:error, :conflict} ->
        # Another node is checking with the same name at the same time.
        :ok

      {:error, _} = error ->
        error
    end
  end

  @impl LeaseLoop.Store
  def heartbeat(s) do
    body = JSON.encode!(%{"heartbeat" => now()})
    {ok(ObjectStore.put(s.os, "nodes/#{s.me}", body, timeout: s.timeout)), s}
  end

  # One conditional write per lease held. The leases held are then those
  # renewed and those whose renewal failed (the next renewal tries them
  # again); a lease the loop no longer renews is forgotten.
  @impl LeaseLoop.Store
  def renew(s, shards) do
    results =
      shards
      |> Task.async_stream(&{&1, renew_one(s, &1)}, max_concurrency: 16, timeout: :infinity)
      |> Enum.map(fn {:ok, result} -> result end)

    renewed = for {n, {:renewed, held}} <- results, into: %{}, do: {n, held}
    failed = for {n, {:failed, _}} <- results, into: %{}, do: {n, Map.fetch!(s.held, n)}
    s = %{s | held: Map.merge(failed, renewed)}

    case for({_n, {:failed, reason}} <- results, do: reason) do
      [] -> {{:ok, Map.keys(renewed)}, s}
      [reason | _] -> {{:error, reason, Map.keys(renewed), Map.keys(failed)}, s}
    end
  end

  defp renew_one(s, n) do
    case Map.fetch(s.held, n) do
      {:ok, {version, generation}} ->
        case put_held(s, n, lease(s.me, now() + s.ttl, generation), version, generation) do
          {:ok, version} -> {:renewed, {version, generation}}
          {:error, :conflict} -> :lost
          {:error, reason} -> {:failed, reason}
        end

      :error ->
        :lost
    end
  end

  # A conditional write on the version this node last wrote. A write that
  # failed (a timeout) may still have been written, so on a conflict the
  # lease is read: it is still this node's if it names this node and the
  # same generation, since a claim by another node changes both.
  defp put_held(s, n, body, version, generation) do
    with {:error, :conflict} <- put(s, n, body, {:update, version}),
         {:ok, {current, version}} <- ObjectStore.get(s.os, key(n), timeout: s.timeout),
         %{owner: owner, generation: ^generation} when owner == s.me <- parse(current) do
      put(s, n, body, {:update, version})
    else
      # No lease, or another node's.
      {:ok, nil} -> {:error, :conflict}
      %{} -> {:error, :conflict}
      :invalid -> {:error, :conflict}
      # The first write's result, or a failed read.
      result -> result
    end
  end

  @impl LeaseLoop.Store
  def live_nodes(s) do
    case ObjectStore.list(s.os, "nodes/", timeout: s.timeout) do
      {:ok, keys} -> {{:ok, max(count_live(s, keys), 1)}, s}
      {:error, _} = error -> {error, s}
    end
  end

  # The nodes whose heartbeat is within the TTL.
  defp count_live(s, keys) do
    since = now() - s.ttl

    keys
    |> Task.async_stream(&ObjectStore.get(s.os, &1, timeout: s.timeout),
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Enum.count(fn
      {:ok, {:ok, {body, _}}} -> heartbeat_since?(body, since)
      _ -> false
    end)
  end

  defp heartbeat_since?(body, since) do
    case JSON.decode(body) do
      {:ok, %{"heartbeat" => t}} when is_integer(t) -> t >= since
      _ -> false
    end
  end

  # Reads every lease; keeps them for claim/2.
  @impl LeaseLoop.Store
  def owners(s) do
    results =
      0..(s.shards - 1)
      |> Task.async_stream(&{&1, ObjectStore.get(s.os, key(&1), timeout: s.timeout)},
        max_concurrency: 16,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, result} -> result end)

    case Enum.find(results, &match?({_, {:error, _}}, &1)) do
      {_, error} ->
        {error, s}

      nil ->
        scan =
          Map.new(results, fn
            {n, {:ok, nil}} -> {n, {nil, nil}}
            {n, {:ok, {body, version}}} -> {n, {parse(body), version}}
          end)

        t = now()

        owners =
          for {n, {%{owner: owner, expires_at: at}, _}} <- scan,
              owner != nil and at > t,
              into: %{},
              do: {n, owner}

        {{:ok, owners}, %{s | scan: scan}}
    end
  end

  # Claims free or expired leases from the last read, in a random order so
  # that nodes claiming at the same time mostly try different ones. The
  # conditional write decides any race. A lease that names this node but
  # is not held here (a claim whose reply was lost, a release that failed,
  # a shard that self-fenced) is not running here: it is claimed at once
  # rather than after it expires.
  @impl LeaseLoop.Store
  def claim(s, count) do
    t = now()

    candidates =
      for {n, {lease, version}} <- s.scan,
          not Map.has_key?(s.held, n),
          claimable?(lease, s.me, t, s.skew),
          do: {n, lease, version}

    {claimed, held, _left} =
      candidates
      |> Enum.shuffle()
      |> Enum.reduce_while({[], s.held, count}, &claim_one(s, &1, &2))

    {{:ok, claimed}, %{s | held: held}}
  end

  defp claim_one(_s, _candidate, {_claimed, _held, 0} = acc), do: {:halt, acc}

  defp claim_one(s, {n, lease, version}, {claimed, held, left}) do
    generation =
      case lease do
        %{generation: g} -> g + 1
        _ -> 1
      end

    body = lease(s.me, now() + s.ttl, generation)
    mode = if lease == nil and version == nil, do: :create, else: {:update, version}

    case put(s, n, body, mode) do
      {:ok, v} ->
        {:cont, {[{n, generation} | claimed], Map.put(held, n, {v, generation}), left - 1}}

      {:error, _} ->
        {:cont, {claimed, held, left}}
    end
  end

  defp claimable?(nil, _me, _t, _skew), do: true
  # Unreadable: overwrite it.
  defp claimable?(:invalid, _me, _t, _skew), do: true
  defp claimable?(%{owner: nil}, _me, _t, _skew), do: true
  defp claimable?(%{owner: me}, me, _t, _skew), do: true
  defp claimable?(%{expires_at: at}, _me, t, skew), do: at + skew < t

  @impl LeaseLoop.Store
  def release(s, n) do
    case Map.pop(s.held, n) do
      {nil, _} ->
        s

      {{version, generation}, held} ->
        put_held(s, n, lease(nil, 0, generation), version, generation)
        %{s | held: held}
    end
  end

  @impl LeaseLoop.Store
  def leave(s) do
    s.held
    |> Map.keys()
    |> Task.async_stream(&release(s, &1), max_concurrency: 16, timeout: :infinity)
    |> Stream.run()

    s = %{s | held: %{}}
    if s.os, do: ObjectStore.delete(s.os, "nodes/#{s.me}", timeout: s.timeout)
    s
  end

  defp put(s, n, body, mode),
    do: ObjectStore.put(s.os, key(n), body, mode: mode, timeout: s.timeout)

  defp key(n), do: "shards/" <> String.pad_leading(Integer.to_string(n), 6, "0")

  defp lease(owner, expires_at, generation),
    do: JSON.encode!(%{"owner" => owner, "expires_at" => expires_at, "generation" => generation})

  # A lease as stored, or :invalid.
  defp parse(body) do
    case JSON.decode(body) do
      {:ok, %{"owner" => owner, "expires_at" => at, "generation" => g}}
      when (is_binary(owner) or is_nil(owner)) and is_integer(at) and is_integer(g) and g >= 0 ->
        %{owner: owner, expires_at: at, generation: g}

      _ ->
        :invalid
    end
  end

  defp now, do: System.os_time(:millisecond)

  defp ok({:ok, _}), do: :ok
  defp ok({:error, _} = error), do: error
end
