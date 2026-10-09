defmodule Slap.Files.Test.FilesCase do
  @moduledoc false
  # Starts Slap.KV.Cluster and Slap.Files on a fresh local directory for each
  # test, with a clock the test moves (`advance/1`) and no timed sweeps or
  # reconciliations: the test runs them (`sweep/0`, `reconcile/0`). Tests
  # are not async: both are named processes. Tests tagged `:s3` store file
  # bodies on an S3-compatible server instead (SLAP_TEST_S3_ENDPOINT,
  # SLAP_TEST_S3_BUCKET); Slap.KV stays local.

  use ExUnit.CaseTemplate

  alias Slap.Files.{Config, Intent, Object, Record, Scan, Sweeper}
  alias Slap.KV
  alias Slap.SlateDB.ObjectStore

  using do
    quote do
      import Slap.Files.Test.FilesCase
      alias Slap.Files
    end
  end

  @start_ms 1_700_000_000_000

  setup context do
    dir = Path.join(System.tmp_dir!(), "slap-files-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(%{
      id: __MODULE__.Clock,
      start: {Agent, :start_link, [fn -> @start_ms end, [name: __MODULE__.Clock]]}
    })

    start_supervised!(
      {Slap.KV.Cluster,
       store: {:local, dir}, path: "kv", shards: 2, settings: %{flush_interval: "2ms"}}
    )

    opts =
      [
        store: if(context[:s3], do: s3_store(), else: {:local, dir}),
        inline_max_bytes: 16,
        inline_limit: 64,
        retention_ms: 1_000,
        upload_timeout_ms: 10_000,
        sweep_interval_ms: :timer.hours(1),
        reconcile_interval_ms: :timer.hours(1),
        max_clock_skew_ms: 0,
        clock: fn -> Agent.get(__MODULE__.Clock, & &1) end
      ]
      |> Keyword.merge(context[:files] || [])

    start_supervised!({Slap.Files, opts})
    {:ok, files_dir: dir}
  end

  defp s3_store do
    endpoint = System.fetch_env!("SLAP_TEST_S3_ENDPOINT")
    bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")

    # The bucket keeps data between test runs, and System.unique_integer/1
    # repeats across VMs, so the prefix is random.
    {:url, "s3://#{bucket}/files/test-#{Slap.Files.new_id()}",
     aws_endpoint: endpoint,
     aws_allow_http: "true",
     aws_region: "us-east-1",
     aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
     aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")}
  end

  @doc "Moves the clock on by `ms`."
  def advance(ms), do: Agent.update(__MODULE__.Clock, &(&1 + ms))

  @doc "Sweeps now."
  def sweep, do: Sweeper.sweep()

  @doc "Reconciles now."
  def reconcile, do: Sweeper.reconcile()

  @doc "Every object in the store."
  def objects do
    {:ok, keys} = ObjectStore.list(Config.get().objects, "objects/")
    Enum.sort(keys)
  end

  @doc "Every registered object key's id."
  def registrations do
    Enum.flat_map(Intent.buckets(), fn bucket ->
      {:ok, ids} =
        Scan.reduce(Object.partition(bucket, Config.get()), [], fn {id, _}, ids -> [id | ids] end)

      ids
    end)
  end

  @doc "The `Slap.KV` partition writer of `partition`."
  def partition_writer(partition) do
    {:ok, {:local, ctx}} = KV.Cluster.lookup(KV.Cluster.shard_for(partition))
    {:ok, i} = KV.PartitionWriter.index(ctx, partition)
    [{pid, _}] = Registry.lookup(ctx.registry, {ctx.n, {:partition_writer, i}})
    pid
  end

  @doc "Every intent."
  def intents do
    Enum.flat_map(Intent.buckets(), fn bucket ->
      {:ok, intents} = Intent.reduce(bucket, [], &[&1 | &2], Config.get())
      intents
    end)
  end

  @doc "The object key of a file's body, or nil."
  def object_key(ref) do
    {:ok, current} = Record.get(ref, [], Config.get())
    with {_version, record} <- current, do: Record.object_key(record)
  end

  @doc "A body as chunks of `size` bytes, for a streamed put."
  def chunks(binary, size) do
    for i <- 0..max(div(byte_size(binary) - 1, size), 0),
        do: binary_part(binary, i * size, min(size, byte_size(binary) - i * size))
  end
end
