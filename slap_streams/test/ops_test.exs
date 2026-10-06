defmodule Slap.Streams.OpsTest do
  use Slap.Streams.Test.ClusterCase, async: false
  import Plug.Test
  import Plug.Conn

  @moduletag :capture_log

  alias Slap.Streams
  alias Slap.Streams.HTTP.Router

  defmodule MetricsCluster do
    use Slap.Streams.Cluster, otp_app: :slap_streams
  end

  defp request(opts, method, path, headers \\ []) do
    conn =
      Enum.reduce(headers, conn(method, path, ""), fn {k, v}, c -> put_req_header(c, k, v) end)

    Router.call(conn, Router.init(opts))
  end

  test "historical reads are cacheable publicly, or privately with :private_cache" do
    # One message per read, so the first read is historical (not up to date).
    assert request([], :put, "/v1/stream/app/c").status == 201

    for body <- ["one", "two"] do
      conn =
        conn(:post, "/v1/stream/app/c", body)
        |> put_req_header("content-type", "application/octet-stream")

      assert Router.call(conn, Router.init([])).status in [200, 204]
    end

    conn = request([max_read: 1], :get, "/v1/stream/app/c?offset=-1")
    assert conn.status == 200
    assert [cache] = get_resp_header(conn, "cache-control")
    assert cache =~ "public"

    # Behind authentication, a shared cache must not keep them.
    conn = request([max_read: 1, private_cache: true], :get, "/v1/stream/app/c?offset=-1")
    assert [cache] = get_resp_header(conn, "cache-control")
    assert cache =~ "private"
    refute cache =~ "public"
  end

  test "stream paths are limited in length" do
    assert request([], :put, "/v1/stream/" <> String.duplicate("a", 600)).status == 414
  end

  test "metrics: Prometheus text from telemetry" do
    {:ok, :created, _} = Streams.create("/m")
    {:ok, _} = Streams.append("/m", "hello")
    {:ok, :created, _} = Streams.create("/v1/stream/m")
    request([], :get, "/v1/stream/m?offset=-1")

    # Load is sampled periodically; send one sample now.
    Streams.Telemetry.execute([:shard, :load], Streams.ShardLoad.snapshot(ctx_for("/m")), %{
      shard: 0
    })

    text = IO.iodata_to_binary(Streams.Metrics.render())

    assert text =~ "# TYPE slap_streams_append_duration_seconds histogram"
    assert text =~ ~s(slap_streams_append_duration_seconds_bucket{le="+Inf"})
    assert text =~ ~r/slap_streams_appends_total\{result="appended"\} \d+/
    assert text =~ ~r/slap_streams_http_requests_total\{method="GET",status="200"\} \d+/
    assert text =~ ~s(slap_streams_stream_servers{shard="0"})
    assert text =~ "slap_streams_store_probe_ok 1"

    conn = Streams.HTTP.Metrics.call(conn(:get, "/metrics"), [])
    assert conn.status == 200
    assert conn.resp_body =~ "slap_streams_appends_total"

    # Methods outside the protocol share one label.
    request([], :propfind, "/v1/stream/m")
    assert IO.iodata_to_binary(Streams.Metrics.render()) =~ ~s(method="OTHER")
    refute IO.iodata_to_binary(Streams.Metrics.render()) =~ ~s(method="PROPFIND")
  end

  test "KV cluster telemetry does not change Streams metrics" do
    :telemetry.execute(
      [:slap, :cluster, :durability, :lag],
      %{lag: 7},
      %{cluster: Slap.KV.Cluster, shard: 9999}
    )

    refute IO.iodata_to_binary(Streams.Metrics.render()) =~
             ~s(slap_streams_durability_lag{shard="9999"})

    :telemetry.execute(
      [:slap, :cluster, :durability, :lag],
      %{lag: 3},
      %{cluster: MetricsCluster, shard: 9998}
    )

    assert IO.iodata_to_binary(Streams.Metrics.render()) =~
             ~s(slap_streams_durability_lag{shard="9998"} 3)
  end
end
