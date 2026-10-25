defmodule Slap.KV.TelemetryTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Slap.KV.Test.WriterControl

  alias Slap.KV.HTTP.Router

  @moduletag :capture_log
  @moduletag settings: %{flush_interval: nil}
  @moduletag shards: 1
  @moduletag child_options: [
               max_inflight_bytes_per_writer: 10,
               max_inflight_requests_per_writer: 2
             ]

  setup do
    id = make_ref()

    events =
      for suffix <- [
            [:request, :rejected],
            [:shard, :load],
            [:write, :acknowledged],
            [:partition_writer, :write_failed],
            [:http, :request]
          ],
          do: [:slap, :kv | suffix]

    :ok = :telemetry.attach_many(id, events, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
  end

  def forward(event, measurements, metadata, test),
    do: send(test, {:event, event, measurements, metadata})

  test "rejections report the limiting budget; confirmations keep slots without mutation acknowledgements" do
    batch = pending("p", fn -> KV.batch("p", [{:put, "a", ""}, {:delete, "b"}]) end)
    assert_receive {:event, [:slap, :kv, :shard, :load], %{bytes: 8, requests: 1}, %{shard: 0}}
    assert {:error, :overloaded} = KV.put("p", "k", "x")
    writer = index("p")

    assert_receive {:event, [:slap, :kv, :request, :rejected], %{bytes: 5, requests: 1},
                    %{
                      partition: "p",
                      shard: 0,
                      writer: ^writer,
                      op: :put,
                      reason: :overloaded,
                      limit: :writer_bytes
                    }}

    get = pending("p", fn -> KV.get("p", "missing", consistency: :linearizable) end)
    assert_receive {:event, [:slap, :kv, :shard, :load], %{bytes: 8, requests: 2}, _}
    assert {:error, :overloaded} = KV.get("p", "missing", consistency: :linearizable)

    assert_receive {:event, [:slap, :kv, :request, :rejected], %{bytes: 0, requests: 1},
                    %{op: :get, limit: :writer_requests}}

    flush("p")
    assert {:ok, _} = Task.await(batch)
    assert {:ok, nil} = Task.await(get)

    assert_receive {:event, [:slap, :kv, :write, :acknowledged], %{duration: duration},
                    %{op: :batch}}

    assert duration >= 0
    refute_received {:event, [:slap, :kv, :write, :acknowledged], _, _}
    refute_received {:event, [:slap, :kv, :partition_writer, :write_failed], _, _}
  end

  @tag settings: %{flush_interval: "2ms"}
  test "HTTP POST telemetry records batch responses" do
    conn =
      Plug.Test.conn(:post, "/v1/kv/p", ~s({"ops":[{"op":"check","key":"aw","if_absent":true}]}))
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call(Router.init([]))

    assert conn.status == 200

    assert_receive {:event, [:slap, :kv, :http, :request], %{duration: duration},
                    %{method: "POST", status: 200}}

    assert duration >= 0
    refute_received {:event, [:slap, :kv, :write, :acknowledged], _, _}
  end
end
