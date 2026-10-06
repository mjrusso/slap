defmodule Slap.SlateDB.TelemetryTest do
  # The telemetry handler receives matching events from every process in this VM.
  use ExUnit.Case, async: false

  alias Slap.SlateDB

  @events [[:slap, :slatedb, :durable], [:slap, :slatedb, :stats], [:slap, :slatedb, :closed]]

  setup do
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    {:ok, db} = SlateDB.open("telemetry", store: :memory)
    %{db: db}
  end

  def forward(event, measurements, metadata, test),
    do: send(test, {:telemetry, event, measurements, metadata})

  test "one supervisor runs telemetry for two databases", %{db: db} do
    {:ok, other} = SlateDB.open("telemetry-other", store: :memory)

    first = start_supervised!({SlateDB.Telemetry, db: db})
    second = start_supervised!({SlateDB.Telemetry, db: other})

    assert first != second
    :ok = SlateDB.close(other)
    :ok = SlateDB.close(db)
  end

  test "emits durable progress, stats, and the close", %{db: db} do
    pid =
      start_supervised!({SlateDB.Telemetry, db: db, metadata: %{name: "t"}, interval: 50})

    ref = Process.monitor(pid)

    {:ok, seq} = SlateDB.put(db, "k", "v")
    :ok = SlateDB.flush(db)

    assert_receive {:telemetry, [:slap, :slatedb, :durable], %{durable_seq: durable},
                    %{name: "t"}}
                   when durable >= seq,
                   5_000

    assert_receive {:telemetry, [:slap, :slatedb, :stats], %{last_write_seq: ^seq}, %{name: "t"}},
                   5_000

    :ok = SlateDB.close(db)

    assert_receive {:telemetry, [:slap, :slatedb, :closed], %{}, %{name: "t", reason: :clean}},
                   5_000

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end
end
