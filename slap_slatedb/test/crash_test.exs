defmodule Slap.SlateDB.CrashTest do
  alias Slap.SlateDB

  # Kills a writer VM with SIGKILL and checks that every write it was told is
  # durable survives. Run with `mix test --include crash`; CI runs it, and it
  # should be run on every SlateDB upgrade.
  #
  # The S3 variant is tagged only :s3, so it runs whenever an S3 endpoint is
  # configured. Tagging it :crash too would make `--include crash` run it
  # without one, because --include wins over the :s3 exclusion.
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  # Kill the writer once this many writes have been reported durable.
  @kill_after 2_000

  setup do
    dir = Path.join(System.tmp_dir!(), "slap-slatedb-crash-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  @tag :crash
  test "writes reported durable survive a SIGKILL", %{dir: dir} do
    run_crash(dir, {:local, dir})
  end

  @tag :s3
  test "writes reported durable survive a SIGKILL, on S3", %{dir: dir} do
    store =
      {:url, "s3://#{System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")}/crash",
       [
         aws_endpoint: System.fetch_env!("SLAP_TEST_S3_ENDPOINT"),
         aws_allow_http: "true",
         aws_region: System.get_env("SLAP_TEST_S3_REGION", "us-east-1"),
         aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
         aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
       ]}

    run_crash(dir, store, "s3")
  end

  defp run_crash(dir, store, store_arg \\ nil) do
    path = "crash-#{System.unique_integer([:positive])}"
    # Every compiled app, so the writer can start :slap_slatedb and its
    # dependencies (rustler_precompiled, when built with Hex).
    code_paths =
      Mix.Project.build_path()
      |> Path.join("lib/*/ebin")
      |> Path.wildcard()
      |> Enum.flat_map(&["-pa", &1])

    elixir = System.find_executable("elixir")
    script = Path.expand("support/crash_writer.exs", __DIR__)

    port =
      Port.open({:spawn_executable, elixir}, [
        :binary,
        :exit_status,
        {:line, 256},
        args: code_paths ++ [script, store_arg || dir, path]
      ])

    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {writes, durable} = read_until_durable(port, %{}, 0)

    # SIGKILL: no flush, no close, no Erlang shutdown.
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
    wait_for_exit(port)

    assert durable >= @kill_after
    assert map_size(writes) > 0

    {:ok, db} = SlateDB.open(path, store: store)

    lost =
      for {i, seq} <- writes,
          seq <= durable,
          SlateDB.get(db, "k#{i}") != {:ok, "v#{i}"},
          do: i

    assert lost == [],
           "#{length(lost)} writes reported durable were lost: #{inspect(Enum.take(lost, 10))}"

    assert SlateDB.durable_seq(db) >= durable
    :ok = SlateDB.close(db)
  end

  # Collects `W i seq` lines until a `D durable` line reports at least
  # @kill_after. Returns the writes seen so far and that durable seq.
  defp read_until_durable(_port, writes, durable) when durable >= @kill_after,
    do: {writes, durable}

  defp read_until_durable(port, writes, durable) do
    receive do
      {^port, {:data, {:eol, "W " <> rest}}} ->
        [i, seq] = String.split(rest)
        read_until_durable(port, Map.put(writes, i, String.to_integer(seq)), durable)

      {^port, {:data, {:eol, "D " <> seq}}} ->
        read_until_durable(port, writes, max(durable, String.to_integer(seq)))

      {^port, {:data, _other}} ->
        read_until_durable(port, writes, durable)

      {^port, {:exit_status, status}} ->
        flunk("the writer exited early with status #{status}")
    after
      60_000 -> flunk("the writer reported no durable writes in 60 s")
    end
  end

  defp wait_for_exit(port) do
    receive do
      {^port, {:exit_status, _}} -> :ok
      {^port, {:data, _}} -> wait_for_exit(port)
    after
      10_000 -> flunk("the writer did not exit after SIGKILL")
    end
  end
end
