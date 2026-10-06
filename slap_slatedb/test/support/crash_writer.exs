# Run by test/crash_test.exs in a separate BEAM, which the test kills with
# SIGKILL while this script is writing.
#
#     elixir -pa _build/test/lib/*/ebin ... test/support/crash_writer.exs STORE PATH
#
# STORE is a local directory, or "s3" to use the SLAP_TEST_S3_* settings.
# The script writes key "k<i>" = "v<i>" for i = 1, 2, ... without waiting for
# durability, and prints:
#
#   W <i> <seq>     after each write returns
#   D <durable_seq> each time a durability notification arrives
#
# It never stops on its own.

[store_arg, path] = System.argv()
{:ok, _} = Application.ensure_all_started(:slap_slatedb)

store =
  case store_arg do
    "s3" ->
      {:url, "s3://#{System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")}/crash",
       [
         aws_endpoint: System.fetch_env!("SLAP_TEST_S3_ENDPOINT"),
         aws_allow_http: "true",
         aws_region: System.get_env("SLAP_TEST_S3_REGION", "us-east-1"),
         aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
         aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
       ]}

    dir ->
      {:local, dir}
  end

{:ok, db} = Slap.SlateDB.open(path, store: store, settings: %{flush_interval: "20ms"})
{:ok, _} = Slap.SlateDB.subscribe(db, :crash)

defmodule CrashWriter do
  def loop(db, i) do
    receive do
      {:slap_slatedb_durable, _ref, :crash, durable} -> IO.puts("D #{durable}")
    after
      0 -> :ok
    end

    {:ok, seq} = Slap.SlateDB.put(db, "k#{i}", "v#{i}")
    IO.puts("W #{i} #{seq}")
    loop(db, i + 1)
  end
end

CrashWriter.loop(db, 1)
