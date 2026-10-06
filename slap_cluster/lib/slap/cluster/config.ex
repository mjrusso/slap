defmodule Slap.Cluster.Config do
  @moduledoc false

  alias Slap.SlateDB

  # Reads and checks a cluster's configuration: the application environment
  # (`config :otp_app, Cluster, ...`) with the options given to `start_link/1`
  # on top.

  @enforce_keys [:cluster, :store, :shards]
  defstruct [
    :cluster,
    :store,
    :shards,
    path: "",
    settings: %{},
    cache: nil,
    merge_operator: nil,
    # Local, filled in by new/2: a module named here would be a compile-time
    # dependency.
    strategy: nil,
    shard_children: nil,
    child_options: [],
    probe: true,
    close_timeout: 30_000,
    lag_interval: 1_000,
    # Set when the cluster starts: what `:cache` resolves to for Slap.SlateDB.open.
    db_cache: nil
  ]

  @keys ~w(store shards path settings cache merge_operator strategy shard_children child_options probe
           close_timeout lag_interval)a

  def load(cluster, otp_app, opts, defaults \\ []) do
    env = if otp_app, do: Application.get_env(otp_app, cluster, []), else: []

    unless Enum.all?([opts, defaults, env], &Keyword.keyword?/1),
      do: raise(ArgumentError, "cluster options must be keyword lists")

    opts = defaults |> Keyword.merge(env) |> Keyword.merge(opts)

    case Keyword.keys(opts) -- @keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError, "unknown options for #{inspect(cluster)}: #{inspect(unknown)}"
    end

    for key <- [:store, :shards], not Keyword.has_key?(opts, key) do
      raise ArgumentError, "#{inspect(cluster)} needs the #{inspect(key)} option"
    end

    config = struct!(__MODULE__, [cluster: cluster] ++ opts)
    check(config)
  end

  @checked [
    :shards,
    :path,
    :settings,
    :cache,
    :strategy,
    :shard_children,
    :child_options,
    :probe,
    :close_timeout,
    :lag_interval
  ]

  defp check(%__MODULE__{} = c) do
    for key <- @checked, not valid?(key, Map.fetch!(c, key)) do
      invalid(c, key, expected(key), Map.fetch!(c, key))
    end

    SlateDB.validate_store!(c.store)
    validate_settings!(c.settings)
    validate_child_options!(c.shard_children, c.child_options)

    %{c | strategy: c.strategy |> normalize_strategy() |> validate_strategy()}
  end

  defp validate_settings!(settings) do
    case SlateDB.validate_settings(settings) do
      :ok -> :ok
      {:error, error} -> raise ArgumentError, ":settings #{error.message}"
    end
  end

  defp validate_child_options!({mod, _fun, _args}, opts) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :validate_child_options!, 1),
      do: mod.validate_child_options!(opts)
  end

  defp validate_child_options!(_children, _opts), do: :ok

  defp valid?(:path, value), do: is_binary(value)
  defp valid?(:settings, value), do: is_map(value)
  defp valid?(:probe, value), do: is_boolean(value)
  defp valid?(:cache, value) when value in [nil, :disabled], do: true

  defp valid?(:cache, opts) when is_list(opts),
    do: Keyword.keyword?(opts) and pos_integer?(opts[:capacity_bytes])

  defp valid?(:strategy, mod) when is_atom(mod), do: true

  defp valid?(:strategy, {mod, opts}),
    do: is_atom(mod) and Keyword.keyword?(opts)

  defp valid?(:shard_children, nil), do: true
  defp valid?(:shard_children, {m, f, a}), do: is_atom(m) and is_atom(f) and is_list(a)
  defp valid?(:shard_children, fun), do: is_function(fun, 1)
  defp valid?(:child_options, value), do: is_list(value) and Keyword.keyword?(value)
  # :shards, :close_timeout, :lag_interval
  defp valid?(_key, value), do: pos_integer?(value)

  defp expected(:path), do: "a string"
  defp expected(:settings), do: "a map"
  defp expected(:probe), do: "a boolean"
  defp expected(:cache), do: "nil, :disabled or [capacity_bytes: n] with n > 0"
  defp expected(:strategy), do: "a module or {module, opts}"
  defp expected(:shard_children), do: "{mod, fun, args} or a 1-arity function"
  defp expected(:child_options), do: "a keyword list"
  defp expected(:shards), do: "a positive integer"
  defp expected(_key), do: "a positive integer (ms)"

  defp pos_integer?(value), do: is_integer(value) and value > 0

  defp normalize_strategy(nil), do: {Slap.Cluster.Strategy.Local, []}
  defp normalize_strategy({_mod, _opts} = strategy), do: strategy
  defp normalize_strategy(mod), do: {mod, []}

  defp validate_strategy({mod, opts} = strategy) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :validate_options, 1) do
      :ok = mod.validate_options(opts)
    end

    unless Code.ensure_loaded?(mod) and
             Enum.all?([child_spec: 1, lookup: 2, handle_shard_down: 3], fn {fun, arity} ->
               function_exported?(mod, fun, arity)
             end) do
      raise ArgumentError, ":strategy #{inspect(mod)} must implement Slap.Cluster.Strategy"
    end

    strategy
  end

  defp invalid(c, key, expected, got) do
    raise ArgumentError,
          "#{inspect(c.cluster)}: #{inspect(key)} must be #{expected}, got: #{inspect(got)}"
  end

  @doc false
  # The database path of shard `n`: `<path>/shard-000` and so on. The width
  # grows with the shard count, and never changes for a given count.
  def shard_path(%__MODULE__{path: path, shards: shards}, n) do
    width = max(3, shards |> Kernel.-(1) |> Integer.to_string() |> byte_size())
    name = "shard-" <> String.pad_leading(Integer.to_string(n), width, "0")
    if path == "", do: name, else: Path.join(path, name)
  end

  @doc false
  def probe_path(%__MODULE__{path: path}) do
    node = node() |> Atom.to_string() |> String.replace(~r/[^\w.@-]/, "_")
    unique = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    Enum.join(Enum.reject([path, "_cluster", "probe", node, unique], &(&1 == "")), "/")
  end

  # Stored once per cluster start, for lookups from any process.
  @doc false
  def put(%__MODULE__{} = config), do: :persistent_term.put({__MODULE__, config.cluster}, config)

  @doc false
  def get(cluster) do
    :persistent_term.get({__MODULE__, cluster}, nil) ||
      raise ArgumentError, "#{inspect(cluster)} is not started"
  end
end
