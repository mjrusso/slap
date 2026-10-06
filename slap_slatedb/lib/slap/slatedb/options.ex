defmodule Slap.SlateDB.Options do
  @moduledoc false
  # Checks options and turns them into the terms the NIF decodes. The NIF
  # decodes atoms into enums and fails with a bare `badarg` on anything
  # else, so every value is checked here first, and a bad one raises an
  # `ArgumentError` that names the option.

  alias Slap.SlateDB

  @merge_operators [:u64_add, :i64_add, :u64_max, :u64_min, :append]
  @log_levels [:debug, :info, :warning, :error, :none]

  @read_keys [:durability, :dirty, :cache_blocks]
  @scan_keys @read_keys ++ [:read_ahead_bytes, :max_fetch_tasks, :order]
  @range_keys [:gte, :gt, :lte, :lt, :prefix, :batch_size]
  # Handled by `Slap.SlateDB.Native.timeout/1`, not passed to the NIF.
  @call_keys [:timeout]

  # `DbReaderOptions` takes these durations as `{secs, nanos}`, unlike
  # `Settings`, which takes strings.
  @durations [:manifest_poll_interval, :checkpoint_lifetime]

  # `value` if it is one of `allowed`.
  def one_of(value, name, allowed) do
    if value in allowed do
      value
    else
      raise ArgumentError,
            "expected #{inspect(name)} to be one of #{inspect(allowed)}, got: #{inspect(value)}"
    end
  end

  # A `:store` option as `{kind, location, env_options, options}`.
  def store(:memory), do: {:memory, "", [], []}
  # For testing `Slap.SlateDB.probe_store/3` only; see there.
  def store(:memory_ignoring_preconditions), do: {:memory_ignoring_preconditions, "", [], []}
  def store({:local, dir}), do: {:local, Path.expand(dir), [], []}
  def store({:url, url}), do: store({:url, url, []})

  def store({:url, url, options}) when is_binary(url) and is_list(options) do
    options = Enum.map(options, fn {k, v} -> {to_string(k), to_string(v)} end)
    {:url, url, env(), options}
  end

  def store(other) do
    raise ArgumentError,
          "expected :store to be :memory, {:local, dir}, {:url, url} or " <>
            "{:url, url, options}, got: #{inspect(other)}"
  end

  # The object store environment variables (`AWS_*`, `AZURE_*`, `GOOGLE_*`),
  # read here so that `System.put_env/2` changes are seen. The NIF cannot see
  # those. Each store type uses the ones it knows.
  defp env do
    for {key, value} <- System.get_env(),
        String.starts_with?(key, ["AWS_", "AZURE_", "GOOGLE_"]) do
      {String.downcase(key), value}
    end
  end

  # A `:settings` map as JSON, or nil for the defaults.
  def settings(nil), do: nil
  def settings(map) when is_map(map), do: JSON.encode!(map)

  def settings(other) do
    raise ArgumentError, "expected :settings to be a map, got: #{inspect(other)}"
  end

  # `Slap.SlateDB.Reader.open/2`'s `:settings`, with durations converted.
  def reader_settings(settings) when is_map(settings) do
    settings
    |> Map.new(fn {key, value} ->
      if key in @durations or key in Enum.map(@durations, &Atom.to_string/1),
        do: {key, duration(key, value)},
        else: {key, value}
    end)
    |> settings()
  end

  def reader_settings(other), do: settings(other)

  defp duration(_key, ms) when is_integer(ms) and ms >= 0,
    do: %{secs: div(ms, 1000), nanos: rem(ms, 1000) * 1_000_000}

  defp duration(key, value) when is_binary(value) do
    case Integer.parse(value) do
      {n, "ms"} when n >= 0 -> duration(key, n)
      {n, "s"} when n >= 0 -> duration(key, n * 1_000)
      {n, "m"} when n >= 0 -> duration(key, n * 60_000)
      {n, "h"} when n >= 0 -> duration(key, n * 3_600_000)
      _ -> raise ArgumentError, "invalid duration for #{inspect(key)}: #{inspect(value)}"
    end
  end

  defp duration(key, value),
    do: raise(ArgumentError, "invalid duration for #{inspect(key)}: #{inspect(value)}")

  def cache(nil), do: :default
  def cache(:disabled), do: :disabled
  def cache(%SlateDB.Cache{resource: cache}), do: {:shared, cache}

  def cache(other) do
    raise ArgumentError,
          "expected :cache to be a Slap.SlateDB.Cache or :disabled, got: #{inspect(other)}"
  end

  def merge_operator(nil), do: nil
  def merge_operator(op), do: one_of(op, :merge_operator, @merge_operators)

  def compaction_filter(nil), do: nil
  def compaction_filter(%SlateDB.CompactionFilter{resource: filter}), do: filter

  def compaction_filter(other) do
    raise ArgumentError, "expected a Slap.SlateDB.CompactionFilter, got: #{inspect(other)}"
  end

  def log_level(level), do: one_of(level, :log_level, @log_levels)

  # Read options as the map the NIF expects. Every key is present; `nil`
  # means "use SlateDB's default".
  def read(opts) do
    opts = Keyword.validate!(opts, @read_keys ++ @call_keys)
    Map.new(@read_keys, &{&1, check(&1, Keyword.get(opts, &1))})
  end

  # Scan options as `{range, prefix, options}`, where `range` is
  # `{lower, lower_inclusive, upper, upper_inclusive}`.
  def scan(opts) do
    opts = Keyword.validate!(opts, @range_keys ++ @scan_keys ++ @call_keys)
    {lower, lower_inclusive} = bound(opts, :gte, :gt, "lower")
    {upper, upper_inclusive} = bound(opts, :lte, :lt, "upper")

    prefix =
      case Keyword.get(opts, :prefix) do
        prefix when is_binary(prefix) or is_nil(prefix) -> prefix
        other -> raise ArgumentError, ":prefix must be a binary, got: #{inspect(other)}"
      end

    scan_opts = Map.new(@scan_keys, &{&1, check(&1, Keyword.get(opts, &1))})
    {{lower, lower_inclusive, upper, upper_inclusive}, prefix, scan_opts}
  end

  defp bound(opts, inclusive_key, exclusive_key, name) do
    case {Keyword.get(opts, inclusive_key), Keyword.get(opts, exclusive_key)} do
      {nil, nil} ->
        {nil, false}

      {key, nil} when is_binary(key) ->
        {key, true}

      {nil, key} when is_binary(key) ->
        {key, false}

      {_, _} ->
        raise ArgumentError,
              "give at most one #{name} bound (#{inspect(inclusive_key)} or " <>
                "#{inspect(exclusive_key)}), as a binary"
    end
  end

  defp check(_key, nil), do: nil
  defp check(:durability, value) when value in [:memory, :remote], do: value
  defp check(:order, value) when value in [:asc, :desc], do: value
  defp check(key, value) when key in [:dirty, :cache_blocks] and is_boolean(value), do: value

  defp check(:read_ahead_bytes, value) when is_integer(value) and value >= 0, do: value
  defp check(:max_fetch_tasks, value) when is_integer(value) and value > 0, do: value

  defp check(key, value) do
    raise ArgumentError, "invalid value for #{inspect(key)}: #{inspect(value)}"
  end
end
