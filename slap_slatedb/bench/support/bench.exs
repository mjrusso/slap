# Helpers shared by the benchmark scripts: picking a store from the
# environment, percentiles, and writing results.
#
# Results are written in the format github-action-benchmark reads for its
# "customSmallerIsBetter" and "customBiggerIsBetter" tools: a JSON list of
# `%{name, unit, value}` maps, optionally with `range` and `extra`.

defmodule Bench do
  @doc """
  The store to benchmark, from `SLAP_BENCH_STORE`:

    * `memory` (default) - the in-memory store.
    * `local` - a new directory under the system temp directory.
    * `s3` - an S3-compatible store at `SLAP_BENCH_S3_ENDPOINT`, bucket
      `SLAP_BENCH_S3_BUCKET` (default `slatedb-bench`), with credentials
      from `SLAP_BENCH_S3_KEY` and `SLAP_BENCH_S3_SECRET` (default
      `rustfsadmin` for both, as for a local RustFS).
  """
  def store do
    case System.get_env("SLAP_BENCH_STORE", "memory") do
      "memory" ->
        :memory

      "local" ->
        dir = Path.join(System.tmp_dir!(), "slatedb-bench-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        {:local, dir}

      "s3" ->
        bucket = System.get_env("SLAP_BENCH_S3_BUCKET", "slatedb-bench")
        endpoint = System.fetch_env!("SLAP_BENCH_S3_ENDPOINT")

        {:url, "s3://#{bucket}/slap-slatedb-bench",
         [
           aws_endpoint: endpoint,
           aws_allow_http: String.starts_with?(endpoint, "http://") |> to_string(),
           aws_region: System.get_env("SLAP_BENCH_S3_REGION", "us-east-1"),
           aws_access_key_id: System.get_env("SLAP_BENCH_S3_KEY", "rustfsadmin"),
           aws_secret_access_key: System.get_env("SLAP_BENCH_S3_SECRET", "rustfsadmin")
         ]}

      other ->
        raise "SLAP_BENCH_STORE must be memory, local or s3, got: #{inspect(other)}"
    end
  end

  @doc "A label for the results, from `SLAP_BENCH_LABEL` or the store."
  def label,
    do: System.get_env("SLAP_BENCH_LABEL", System.get_env("SLAP_BENCH_STORE", "memory"))

  @doc "Where results go, from `SLAP_BENCH_OUT` (default `bench/results`)."
  def out_dir do
    dir = System.get_env("SLAP_BENCH_OUT", "bench/results")
    File.mkdir_p!(dir)
    dir
  end

  @doc """
  A multiplier for the amount of work, from `SLAP_BENCH_SCALE` (default
  1.0). Use a small value such as 0.1 to check that the scripts run.
  """
  def scale(n) do
    factor = System.get_env("SLAP_BENCH_SCALE", "1") |> Float.parse() |> elem(0)
    max(1, round(n * factor))
  end

  @doc "The `p`th percentile (0-100) of a list of numbers, by nearest rank."
  def percentile([], _p), do: nil

  def percentile(values, p) do
    sorted = Enum.sort(values)
    rank = max(1, ceil(p / 100 * length(sorted)))
    Enum.at(sorted, rank - 1)
  end

  @doc "Microseconds from `native` time units."
  def us(native), do: System.convert_time_unit(native, :native, :microsecond)

  @doc "Milliseconds, as a float with 0.1 ms resolution, from `native` time units."
  def ms(native), do: Float.round(us(native) / 1000, 1)

  @doc "The total number of object store requests the database has made."
  def object_store_requests(db) do
    for %{name: "slatedb.object_store.request_count", value: n} <- Slap.SlateDB.metrics(db),
        reduce: 0,
        do: (acc -> acc + n)
  end

  @doc """
  Writes `results` to `<out_dir>/<name>-smaller.json` and
  `<out_dir>/<name>-bigger.json` for github-action-benchmark (each only if
  it has entries), and a markdown table to `<out_dir>/<name>.md`.

  Each result is `%{name, unit, value, better: :smaller | :bigger}`, with
  optional `:extra` (a string shown in the chart tooltip).
  """
  def write_results(name, title, results) do
    dir = out_dir()
    name = String.replace(name, ~r/[^\w.-]+/, "_")

    for better <- [:smaller, :bigger] do
      entries =
        for r <- results, r.better == better do
          Map.take(r, [:name, :unit, :value, :extra]) |> Map.reject(fn {_, v} -> is_nil(v) end)
        end

      # github-action-benchmark rejects an empty list.
      if entries != [],
        do: File.write!(Path.join(dir, "#{name}-#{better}.json"), JSON.encode!(entries))
    end

    rows =
      for r <- results do
        arrow = if r.better == :smaller, do: "lower is better", else: "higher is better"
        "| #{r.name} | #{format(r.value)} #{r.unit} | #{arrow} |"
      end

    File.write!(
      Path.join(dir, "#{name}.md"),
      Enum.join(["### #{title}", "", "| Benchmark | Result | |", "|---|---:|---|" | rows], "\n") <>
        "\n\n"
    )

    for r <- results, do: IO.puts("#{r.name}: #{format(r.value)} #{r.unit}")
    :ok
  end

  defp format(value) when is_float(value), do: :erlang.float_to_binary(value, decimals: 1)
  defp format(value), do: to_string(value)
end
