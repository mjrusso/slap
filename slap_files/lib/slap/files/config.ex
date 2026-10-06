defmodule Slap.Files.Config do
  @moduledoc false
  use GenServer
  alias Slap.SlateDB.ObjectStore

  # A literal module atom keeps Config independent of Slap.Files at compile time.
  @default_name :"Elixir.Slap.Files"
  @default_namespace "default"

  @enforce_keys [:objects]
  defstruct [
    :objects,
    name: @default_name,
    cluster: Slap.KV.Cluster,
    namespace: @default_namespace,
    timeout: nil,
    inline_max_bytes: 16 * 1024,
    inline_limit: 1024 * 1024,
    retention_ms: :timer.minutes(5),
    upload_timeout_ms: :timer.hours(1),
    sweep_interval_ms: :timer.seconds(10),
    max_clock_skew_ms: :timer.seconds(30),
    reconcile_interval_ms: :timer.hours(1),
    clock: nil
  ]

  @type t :: %__MODULE__{objects: ObjectStore.t()}

  @options [
    :inline_max_bytes,
    :inline_limit,
    :retention_ms,
    :upload_timeout_ms,
    :sweep_interval_ms,
    :max_clock_skew_ms,
    :reconcile_interval_ms,
    :clock,
    :name,
    :cluster,
    :namespace,
    :timeout
  ]

  @spec new(ObjectStore.t(), keyword()) :: t()
  def new(objects, opts) do
    validate_options!(opts)
    struct!(%__MODULE__{objects: objects}, Keyword.take(opts, @options))
  end

  @doc false
  def validate_options!(opts) do
    validate_namespace!(opts)

    for key <- [:inline_max_bytes, :inline_limit, :retention_ms, :max_clock_skew_ms],
        do: validate_option!(opts, key, &nonneg_integer?/1, "a non-negative integer")

    for key <- [:upload_timeout_ms, :sweep_interval_ms, :reconcile_interval_ms],
        do: validate_option!(opts, key, &positive_integer?/1, "a positive integer")

    validate_option!(opts, :path, &is_binary/1, "a binary")

    validate_option!(
      opts,
      :timeout,
      &valid_timeout?/1,
      "nil, a non-negative integer or :infinity"
    )

    validate_option!(opts, :cluster, &module?/1, "a module")
    validate_option!(opts, :clock, &valid_clock?/1, "a zero-arity function or nil")

    if Keyword.get(opts, :inline_max_bytes, 16 * 1024) >
         Keyword.get(opts, :inline_limit, 1024 * 1024),
       do: raise(ArgumentError, ":inline_max_bytes must not exceed :inline_limit")
  end

  defp validate_namespace!(opts) do
    name = Keyword.get(opts, :name, @default_name)

    unless is_atom(name) and name != nil,
      do: raise(ArgumentError, ":name must be a module atom")

    namespace =
      case Keyword.fetch(opts, :namespace) do
        {:ok, namespace} -> namespace
        :error when name == @default_name -> @default_namespace
        :error -> raise ArgumentError, ":namespace is required for named instances"
      end

    if not is_binary(namespace) or namespace == "",
      do: raise(ArgumentError, ":namespace must be a nonempty binary")
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp nonneg_integer?(value), do: is_integer(value) and value >= 0
  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp valid_timeout?(value), do: value in [nil, :infinity] or nonneg_integer?(value)
  defp module?(value), do: is_atom(value) and value != nil
  defp valid_clock?(value), do: is_nil(value) or is_function(value, 0)

  def child_spec(config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}}
  end

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config_name(config.name))
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    :persistent_term.put(config_name(config.name), config)
    {:ok, config}
  end

  @impl true
  def terminate(_reason, config) do
    :persistent_term.erase(config_name(config.name))
    :ok
  end

  @spec get() :: t()
  def get(name \\ @default_name) do
    case :persistent_term.get(config_name(name), nil) do
      nil -> raise ArgumentError, "Slap.Files instance #{inspect(name)} is not started"
      config -> config
    end
  end

  def sweeper(name), do: Module.concat(name, Sweeper)
  defp config_name(name), do: Module.concat(name, Config)

  def route_opts(config) do
    [cluster: config.cluster] ++ if(config.timeout, do: [timeout: config.timeout], else: [])
  end

  def object_opts(config), do: if(config.timeout, do: [timeout: config.timeout], else: [])

  def partition(%__MODULE__{namespace: namespace}, family, suffix),
    do: "files-ns:#{Base.url_encode64(namespace, padding: false)}:#{family}:#{suffix}"

  def object_prefix(%__MODULE__{namespace: namespace}, bucket),
    do: "objects/ns/#{Base.url_encode64(namespace, padding: false)}/#{bucket}/"

  @doc false
  # Wall-clock milliseconds, for deadlines; tests replace the clock.
  @spec now(t()) :: integer()
  def now(config) do
    case config.clock do
      nil -> System.system_time(:millisecond)
      clock -> clock.()
    end
  end

  @doc false
  # The system time when `now/1` will be `ms`, for a `Slap.KV` deadline:
  # Slap.KV checks deadlines against the system clock.
  @spec system_time(integer(), t()) :: integer()
  def system_time(ms, config), do: System.os_time(:millisecond) + (ms - now(config))
end
