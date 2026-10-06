defmodule Slap.SnapshotLog.Test.ClusterCase do
  @moduledoc false
  # Tests are not async: the cluster is a named process.

  use ExUnit.CaseTemplate

  alias Slap.SnapshotLog

  using do
    quote do
      import Slap.SnapshotLog.Test.ClusterCase
      alias Slap.SnapshotLog
    end
  end

  setup do
    dir =
      Path.join(System.tmp_dir!(), "slap-snapshot-log-test-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.Streams.Cluster, store: {:local, dir}, shards: 2, settings: %{flush_interval: "2ms"}}
    )

    :ok
  end

  def base, do: "/v1/stream/test/log-#{System.unique_integer([:positive])}"

  def load(base) do
    {:reset, %{snapshot: snapshot, offset: offset}} = SnapshotLog.next(base, nil)
    {entries, tail} = read_to_tail(base, offset, [])
    {snapshot, entries, tail}
  end

  defp read_to_tail(base, offset, acc) do
    case SnapshotLog.next(base, offset, wait: 0) do
      {:ok, %{entries: entries, offset: next, up_to_date: true}} -> {acc ++ entries, next}
      {:ok, %{entries: entries, offset: next}} -> read_to_tail(base, next, acc ++ entries)
    end
  end
end
