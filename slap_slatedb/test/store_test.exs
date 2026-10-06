defmodule Slap.SlateDB.StoreTest do
  alias Slap.SlateDB

  # Not async: these tests change process-wide environment variables.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @aws_vars ~w(AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_ENDPOINT
               AWS_ALLOW_HTTP AWS_CONDITIONAL_PUT)

  setup do
    saved = Map.new(@aws_vars, &{&1, System.get_env(&1)})

    on_exit(fn ->
      for {var, value} <- saved do
        if value, do: System.put_env(var, value), else: System.delete_env(var)
      end
    end)
  end

  describe "S3 configuration" do
    test "unknown option keys are rejected" do
      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("x", store: {:url, "s3://bucket", [aws_regoin: "us-east-1"]})

      assert message =~ "aws_regoin"
    end

    test "conditional puts cannot be turned off" do
      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("x", store: {:url, "s3://bucket", [conditional_put: "disabled"]})

      assert message =~ "conditional_put"

      System.put_env("AWS_CONDITIONAL_PUT", "disabled")

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.open("x", store: {:url, "s3://bucket"})
    end

    test "only object storage URLs are supported" do
      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("x", store: {:url, "file:///tmp/slatedb"})

      assert message =~ "s3://, az:// or gs://"
    end

    test "Azure and GCS options are checked too" do
      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("x", store: {:url, "az://container", [azure_acount_name: "x"]})

      assert message =~ "azure_acount_name"

      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("x", store: {:url, "gs://bucket", [google_servce_account: "x"]})

      assert message =~ "google_servce_account"
    end
  end

  describe "probe_store/3" do
    test "passes on the in-memory store" do
      assert {:ok, steps} = SlateDB.probe_store(:memory, "probe")

      assert steps == [
               create: :ok,
               create_again: :ok,
               stale_if_match: :ok,
               current_if_match: :ok,
               delete: :ok
             ]
    end

    test "passes on a local directory, which has no If-Match" do
      dir = Path.join(System.tmp_dir!(), "slatedb-probe-#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(dir) end)

      assert {:ok, steps} = SlateDB.probe_store({:local, dir}, "probe/one")
      assert steps[:create_again] == :ok
      assert steps[:stale_if_match] == :unsupported
      assert steps[:current_if_match] == :unsupported
      assert steps[:delete] == :ok
    end

    test "fails on a store that ignores conditional writes" do
      assert {:error, {:probe_failed, steps}} =
               SlateDB.probe_store(:memory_ignoring_preconditions, "probe")

      assert steps[:create] == :ok
      assert {:failed, message} = steps[:create_again]
      assert message =~ "overwrote"
      assert {:failed, _} = steps[:stale_if_match]
      assert steps[:delete] == :ok
    end
  end

  # These run against Azurite, the Azure Storage emulator. See the README.
  describe "against Azure" do
    @describetag :azure

    setup do
      container = System.get_env("SLAP_TEST_AZURITE_CONTAINER", "slatedb")
      store = {:url, "az://#{container}/slap-slatedb", [azure_storage_use_emulator: "true"]}
      path = "test-#{System.unique_integer([:positive])}"
      settings = %{manifest_poll_interval: "100ms", object_store_max_retries: 0}
      %{path: path, store: store, settings: settings}
    end

    test "the probe passes, including If-Match", ctx do
      assert {:ok, steps} = SlateDB.probe_store(ctx.store, "probe/#{ctx.path}")
      assert Enum.all?(steps, &match?({_, :ok}, &1))
    end

    test "writes durably, reopens and fences a second writer", ctx do
      {:ok, first} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      {:ok, %{ref: ref}} = SlateDB.subscribe(first, :first)
      {:ok, _} = SlateDB.put(first, "k", "v", await_durable: true)

      {:ok, second} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      assert_receive {:slap_slatedb_closed, ^ref, :first, :fenced}, 2_000

      assert {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} =
               SlateDB.put(first, "k", "stale")

      assert {:ok, "v"} = SlateDB.get(second, "k", durability: :remote)
      :ok = SlateDB.close(second)
      assert :ok = SlateDB.close(first)
    end
  end

  # These run against a real S3-compatible server. See the README for how to
  # start RustFS and set SLAP_TEST_S3_ENDPOINT.
  describe "against S3" do
    @describetag :s3

    setup do
      endpoint = System.fetch_env!("SLAP_TEST_S3_ENDPOINT")
      bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")

      # Configure only through the environment, to check that it is read.
      System.put_env("AWS_ENDPOINT", endpoint)
      System.put_env("AWS_ALLOW_HTTP", "true")
      System.put_env("AWS_REGION", System.get_env("SLAP_TEST_S3_REGION", "us-east-1"))
      System.put_env("AWS_ACCESS_KEY_ID", System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"))

      System.put_env(
        "AWS_SECRET_ACCESS_KEY",
        System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
      )

      path = "test-#{System.unique_integer([:positive])}"
      store = {:url, "s3://#{bucket}/slap-slatedb"}
      settings = %{manifest_poll_interval: "100ms", object_store_max_retries: 0}
      %{path: path, store: store, settings: settings}
    end

    test "the probe passes, including If-Match", ctx do
      assert {:ok, steps} = SlateDB.probe_store(ctx.store, "probe/#{ctx.path}")
      assert Enum.all?(steps, &match?({_, :ok}, &1))
    end

    test "opens, writes durably and reads back", ctx do
      {:ok, db} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      {:ok, seq} = SlateDB.put(db, "k", "v", await_durable: true)
      assert SlateDB.durable_seq(db) >= seq
      :ok = SlateDB.close(db)

      {:ok, db} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      assert {:ok, "v"} = SlateDB.get(db, "k", durability: :remote)
      :ok = SlateDB.close(db)
    end

    test "a second writer fences the first", ctx do
      {:ok, first} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      {:ok, %{ref: ref}} = SlateDB.subscribe(first, :first)
      {:ok, _} = SlateDB.put(first, "k", "first", await_durable: true)

      {:ok, second} = SlateDB.open(ctx.path, store: ctx.store, settings: ctx.settings)
      assert_receive {:slap_slatedb_closed, ^ref, :first, :fenced}, 2_000

      assert {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} =
               SlateDB.put(first, "k", "stale")

      assert {:ok, "first"} = SlateDB.get(second, "k")
      :ok = SlateDB.close(second)
      assert :ok = SlateDB.close(first)
    end
  end
end
