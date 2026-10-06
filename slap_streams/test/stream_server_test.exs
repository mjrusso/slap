defmodule Slap.Streams.Test.OtherCluster do
  use Slap.Streams.Cluster, otp_app: :slap_streams
end

defmodule Slap.Streams.StreamServerTest do
  use Slap.Streams.Test.ClusterCase, async: false

  @moduletag :capture_log

  alias Slap.Cluster.Config
  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Offset
  alias Slap.Streams.Test.OtherCluster
  alias Slap.Streams.Wait

  test "invalid request data returns a bad request without creating a stream" do
    assert {:error, {:bad_request, :invalid_path}} = Streams.create(:path)
    assert {:error, {:bad_request, :invalid_ttl}} = Streams.create("/invalid", ttl_s: "10")
    assert {:error, {:bad_request, :invalid_body}} = Streams.append("/invalid", :body)

    assert {:error, {:bad_request, :invalid_producer}} =
             Streams.append("/invalid", "x", producer: {"p", "0", 0})

    assert {:error, {:bad_request, :invalid_offset}} = Streams.read("/invalid", -1)
    assert {:error, {:bad_request, :invalid_offset}} = Streams.trim("/invalid", :offset)
    assert {:error, {:bad_request, :invalid_path}} = Streams.list(:prefix)
    assert {:error, :not_found} = Streams.head("/invalid")
  end

  test "invalid control options raise at the caller" do
    assert_raise ArgumentError, ~r/:timeout/, fn -> Streams.head("/x", timeout: -1) end
    assert_raise ArgumentError, ~r/:max_bytes/, fn -> Streams.read("/x", 0, max_bytes: 0) end
    assert_raise ArgumentError, ~r/:peek/, fn -> Streams.read("/x", 0, peek: true) end
    assert_raise ArgumentError, ~r/:cluster/, fn -> Streams.create("/x", cluster: 1) end

    assert_raise ArgumentError, ~r/:placement_key/, fn ->
      Streams.create("/x", placement_key: 1)
    end

    assert {:ok, :created, _} = Streams.create("/control-wait")
    assert {:ok, {:waiting, pending}} = Streams.wait("/control-wait", :now)

    assert_raise ArgumentError, ~r/timeout/, fn ->
      Streams.await_wake("/control-wait", pending, -1)
    end

    assert_raise ArgumentError, ~r/:timeout/, fn ->
      Streams.cancel_wait("/control-wait", pending, timeout: -1)
    end

    assert :ok = Streams.cancel_wait("/control-wait", pending)
  end

  test "invalid shard options fail before the cluster starts" do
    assert_raise ArgumentError, ~r/:idle_tiemout/, fn ->
      OtherCluster.start_link(
        store: :memory,
        shards: 1,
        child_options: [idle_tiemout: 1_000]
      )
    end

    for {key, value} <- [
          idle_timeout: "5m",
          max_fork_copy_bytes: -1,
          fork_copy_grace: -1,
          expiry_interval: 0,
          repair_interval: 0,
          deleter_interval: 0,
          deleter_page: 0,
          load_interval: 0,
          max_inflight_bytes_per_stream: 0,
          max_inflight_bytes_per_shard: 0
        ] do
      error =
        assert_raise ArgumentError, fn ->
          OtherCluster.start_link(
            store: :memory,
            shards: 1,
            child_options: [{key, value}]
          )
        end

      assert Exception.message(error) =~ Atom.to_string(key)
    end

    start_supervised!(
      {OtherCluster,
       store: :memory, shards: 1, child_options: [max_fork_copy_bytes: 0, fork_copy_grace: 0]}
    )
  end

  test "a second cluster keeps its streams separate" do
    other = OtherCluster
    start_supervised!({other, store: :memory, shards: 1})

    assert {:ok, :created, _} = Streams.create("/same", cluster: other)
    assert {:error, :not_found} = Streams.head("/same")
    assert {:ok, _} = Streams.append("/same", "other", cluster: other)
    assert {:ok, %{messages: [{0, "other"}]}} = Streams.read("/same", 0, cluster: other)
    assert {:ok, :created, _} = Streams.create("/copy", forked_from: "/same", cluster: other)
    assert {:ok, %{messages: [{0, "other"}]}} = Streams.read("/copy", 0, cluster: other)
    assert {:ok, :created, _} = Streams.create("/same")
    assert {:ok, %{messages: []}} = Streams.read("/same", 0)

    assert {:ok, {:waiting, pending}} = Streams.wait("/same", :now, self(), cluster: other)
    ref = Wait.ref(pending)

    assert :ok = Streams.cancel_wait("/same", pending)
    assert {:ok, _} = Streams.append("/same", "later", cluster: other)
    refute_receive {:slap_streams_wake, ^ref, _}, 100
  end

  test "a stream group (paths that differ from a dot segment on) is placed as one" do
    assert Streams.placement_key("/v1/stream/a/b") == "/v1/stream/a/b"
    assert Streams.placement_key("/v1/stream/yjs/s/docs/d/.updates") == "/v1/stream/yjs/s/docs/d"

    assert Streams.placement_key("/v1/stream/yjs/s/docs/d/.snapshots/1_snapshot") ==
             "/v1/stream/yjs/s/docs/d"

    # Reached by path alone, in-process as over HTTP.
    {:ok, :created, _} = Streams.create("/g/.one")
    assert {:ok, _} = Streams.head("/g/.one", placement_key: "/g")
  end

  test "list/2 lists a group's streams under a prefix, in order" do
    for path <- ["/g/.s/b", "/g/.s/a", "/g/.s/c", "/g/.t", "/g/.sx"] do
      {:ok, :created, _} = Streams.create(path)
    end

    :ok = Streams.delete("/g/.s/c")
    assert {:ok, ["/g/.s/a", "/g/.s/b"]} = Streams.list("/g/.s/")
    assert {:ok, ["/g/.s/a", "/g/.s/b", "/g/.sx"]} = Streams.list("/g/.s")
    assert {:ok, []} = Streams.list("/h/.s/")

    assert_raise ArgumentError, fn ->
      Streams.list("/g/.s", placement_key: "/other")
    end
  end

  test "seal/2 stops creates and forks in a group, and leaves its streams" do
    {:ok, :created, _} = Streams.create("/s/.a", content_type: "text/plain")
    {:ok, :created, _} = Streams.create("/src", content_type: "text/plain")
    :ok = Streams.seal("/s")

    assert {:error, :sealed} = Streams.create("/s/.b", content_type: "text/plain")
    assert {:error, :sealed} = Streams.create("/s/.c", forked_from: "/src")
    assert {:ok, :exists, _} = Streams.create("/s/.a", content_type: "text/plain")
    assert {:ok, _} = Streams.append("/s/.a", "x", content_type: "text/plain")
    assert {:ok, ["/s/.a"]} = Streams.list("/s/.")

    assert_raise ArgumentError, fn ->
      Streams.seal("/s", placement_key: "/other")
    end

    # The failed fork is not left registered with its source.
    :ok = Streams.delete("/src")
    assert {:error, :not_found} = Streams.head("/src")
  end

  describe "create (§5.1)" do
    test "creates once, then matches or conflicts on configuration" do
      assert {:ok, :created, %{next_offset: 0, closed: false, content_type: "text/plain"}} =
               Streams.create("/a", content_type: "text/plain")

      assert {:ok, :exists, _} = Streams.create("/a", content_type: "text/plain; charset=utf-8")
      assert {:ok, :exists, _} = Streams.create("/a", content_type: "TEXT/PLAIN")
      assert {:error, :conflict} = Streams.create("/a", content_type: "application/json")
      assert {:error, :conflict} = Streams.create("/a", content_type: "text/plain", ttl_s: 60)
      assert {:error, :conflict} = Streams.create("/a", content_type: "text/plain", closed: true)
    end

    test "defaults to application/octet-stream and rejects TTL with Expires-At" do
      assert {:ok, :created, %{content_type: "application/octet-stream"}} = Streams.create("/b")

      assert {:error, {:bad_request, :ttl_and_expires_at}} =
               Streams.create("/c", ttl_s: 10, expires_at_ms: 1)
    end

    test "with an initial body, and created closed" do
      assert {:ok, :created, %{next_offset: 9}} = Streams.create("/d", body: "hello")
      assert {:ok, read} = Streams.read("/d", 0)
      assert bodies(read) == ["hello"]

      assert {:ok, :created, %{closed: true, next_offset: 8}} =
               Streams.create("/e", body: "done", closed: true)

      assert {:ok, :exists, _} = Streams.create("/e", body: "done", closed: true)
      assert {:error, :conflict} = Streams.create("/e", body: "done")
      assert {:error, {:closed, 8}} = Streams.append("/e", "more")
    end

    test "JSON: initial arrays are flattened, and [] creates an empty stream" do
      json = [content_type: "application/json"]
      assert {:ok, :created, %{next_offset: 0}} = Streams.create("/j1", [body: "[]"] ++ json)

      # 4 + 1, then 4 + 7.
      assert {:ok, :created, %{next_offset: 16}} =
               Streams.create("/j2", [body: ~s([1, {"a":2}])] ++ json)

      assert {:ok, read} = Streams.read("/j2", 0)
      assert bodies(read) == ["1", ~s({"a":2})]
      assert {:error, {:bad_request, :invalid_json}} = Streams.create("/j3", [body: "{"] ++ json)
    end
  end

  describe "append (§5.2)" do
    setup do
      {:ok, :created, _} = Streams.create("/s", content_type: "text/plain")
      :ok
    end

    test "appends and reports the new tail" do
      assert {:ok, %{result: :appended, next_offset: 7, closed: false, producer: nil}} =
               Streams.append("/s", "abc")

      assert {:ok, %{next_offset: 13}} = Streams.append("/s", "de", content_type: "text/plain")
      assert {:ok, %{next_offset: 13}} = Streams.head("/s")
      assert {:ok, read} = Streams.read("/s", 0)
      assert read.messages == [{0, "abc"}, {7, "de"}]
      assert %{next_offset: 13, up_to_date: true, closed: false} = read
    end

    test "errors" do
      assert {:error, :not_found} = Streams.append("/missing", "x")
      assert {:error, {:bad_request, :empty_body}} = Streams.append("/s", "")
      assert {:error, :content_type_mismatch} = Streams.append("/s", "x", content_type: "a/b")
    end

    test "Stream-Seq must increase, bytewise" do
      assert {:ok, _} = Streams.append("/s", "1", stream_seq: "b")
      assert {:error, :stream_seq_conflict} = Streams.append("/s", "2", stream_seq: "b")
      assert {:error, :stream_seq_conflict} = Streams.append("/s", "2", stream_seq: "a")
      assert {:ok, _} = Streams.append("/s", "2", stream_seq: "ba")
      assert {:ok, _} = Streams.append("/s", "3")
    end

    test "closing: with the last append, then rejecting appends" do
      assert {:ok, %{closed: true, next_offset: 7}} = Streams.append("/s", "end", close: true)
      # Closed comes before a content type mismatch (§5.2 precedence).
      assert {:error, {:closed, 7}} = Streams.append("/s", "x", content_type: "a/b")
      assert {:ok, %{result: :closed, closed: true, next_offset: 7}} = Streams.close("/s")
      assert {:ok, %{closed: true}} = Streams.head("/s")
    end

    test "JSON appends are validated and flattened" do
      {:ok, :created, _} = Streams.create("/json", content_type: "application/json")
      assert {:error, {:bad_request, :empty_array}} = Streams.append("/json", "[]")
      assert {:error, {:bad_request, :invalid_json}} = Streams.append("/json", "[1,")
      assert {:ok, %{next_offset: 10}} = Streams.append("/json", "[1, 2]")
      assert {:ok, %{next_offset: 22}} = Streams.append("/json", ~s({"x": 1}))
      assert {:ok, read} = Streams.read("/json", 0)
      assert bodies(read) == ["1", "2", ~s({"x": 1})]
    end
  end

  describe "idempotent producers (§5.2.1)" do
    setup do
      {:ok, :created, _} = Streams.create("/p")
      :ok
    end

    defp pa(body, id, epoch, seq, opts \\ []),
      do: Streams.append("/p", body, [producer: {id, epoch, seq}] ++ opts)

    test "sequence numbers, duplicates and gaps" do
      assert {:error, {:producer_seq_gap, 0, 1}} = pa("x", "w", 0, 1)
      assert {:ok, %{result: :appended, producer: {0, 0}, next_offset: 5}} = pa("a", "w", 0, 0)
      assert {:ok, %{result: :appended, producer: {0, 1}}} = pa("b", "w", 0, 1)
      assert {:ok, %{result: :duplicate, producer: {0, 1}, next_offset: 10}} = pa("a", "w", 0, 0)
      assert {:error, {:producer_seq_gap, 2, 4}} = pa("d", "w", 0, 4)
      assert {:ok, read} = Streams.read("/p", 0)
      assert bodies(read) == ["a", "b"]
    end

    test "a fenced server does not answer a sequence gap from its stale producer state" do
      path = "/fenced"
      {:ok, :created, _} = Streams.create(path, content_type: "text/plain")
      {:ok, _} = Streams.append(path, "a", producer: {"p", 0, 0})

      # Another writer opens the shard's database, as a node that took the
      # shard over would; it may have accepted seq 1 there.
      config = Config.get(Streams.Cluster)
      shard_path = Config.shard_path(config, ctx_for(path).n)
      {:ok, other} = SlateDB.open(shard_path, store: config.store, settings: config.settings)
      on_exit(fn -> SlateDB.close(other) end)

      assert {:error, :unavailable} = Streams.append(path, "c", producer: {"p", 0, 2})
    end

    test "epochs fence zombies" do
      {:ok, _} = pa("a", "w", 0, 0)
      assert {:error, {:bad_request, :new_epoch_must_start_at_zero}} = pa("b", "w", 1, 1)
      assert {:ok, %{result: :appended, producer: {1, 0}}} = pa("b", "w", 1, 0)
      assert {:error, {:stale_epoch, 1}} = pa("c", "w", 0, 1)
    end

    test "a duplicate is checked before Stream-Seq" do
      {:ok, _} = pa("a", "w", 0, 0, stream_seq: "5")
      assert {:ok, %{result: :duplicate}} = pa("a", "w", 0, 0, stream_seq: "5")
    end

    test "a retry of the closing append is a duplicate, others conflict" do
      assert {:ok, %{closed: true}} = pa("last", "w", 0, 0, close: true)
      assert {:ok, %{result: :duplicate, closed: true}} = pa("last", "w", 0, 0, close: true)
      assert {:error, {:closed, 8}} = pa("other", "w", 0, 1)
      assert {:error, {:closed, 8}} = pa("other", "v", 0, 0)
    end

    test "close-only with a producer" do
      {:ok, _} = pa("a", "w", 0, 0)

      assert {:ok, %{result: :closed, producer: {0, 1}}} =
               Streams.close("/p", producer: {"w", 0, 1})

      assert {:ok, %{result: :duplicate, closed: true}} =
               Streams.close("/p", producer: {"w", 0, 1})

      assert {:error, {:closed, _}} = Streams.close("/p", producer: {"w", 0, 2})
    end
  end

  describe "reads (§5.6, §8)" do
    setup do
      {:ok, :created, _} = Streams.create("/r")
      for body <- ["one", "two", "three"], do: {:ok, _} = Streams.append("/r", body)
      :ok
    end

    test "from any message boundary, paged by max_bytes" do
      assert {:ok, %{messages: [{0, "one"}], next_offset: 7, up_to_date: false}} =
               Streams.read("/r", 0, max_bytes: 1)

      assert {:ok, %{messages: [{7, "two"}, {14, "three"}], up_to_date: true}} =
               Streams.read("/r", 7)

      assert {:ok, %{messages: [], next_offset: 23, up_to_date: true}} = Streams.read("/r", 23)
      assert {:ok, %{messages: [], next_offset: 23}} = Streams.read("/r", :now)
      assert {:error, :offset_beyond_tail} = Streams.read("/r", 24)
      assert {:error, :not_found} = Streams.read("/nope", 0)
    end

    test "a database that closes under a read gives :unavailable, not a crash" do
      ctx = ctx_for("/r")
      # As when the shard stops (it moved, or was fenced) during a read.
      :ok = SlateDB.close(ctx.db)
      assert {:error, :unavailable} = Streams.Reader.read(ctx, "/r", 0, 1_000, 5_000)
    end

    test "Stream-Closed only with the final data" do
      {:ok, _} = Streams.close("/r")
      assert {:ok, %{closed: false}} = Streams.read("/r", 0, max_bytes: 1)
      assert {:ok, %{closed: true}} = Streams.read("/r", 7)
      assert {:ok, %{closed: true, messages: []}} = Streams.read("/r", :now)
    end

    test "offsets use the official wire format" do
      {:ok, %{next_offset: n}} = Streams.head("/r")
      assert Offset.encode(n) == "0000000000000000_0000000000000023"
    end
  end

  describe "waiting (long-poll)" do
    setup do
      {:ok, :created, _} = Streams.create("/w")
      :ok
    end

    test "wakes on data, on close and on delete" do
      wait = &Streams.wait/3
      bad_pid = Process.get(:bad_pid, :not_a_pid)

      assert_raise ArgumentError, fn ->
        wait.("/w", 0, bad_pid)
      end

      assert {:ok, {:waiting, pending}} = Streams.wait("/w", 0)
      ref = Wait.ref(pending)
      assert {:ok, {:waiting, pending}} = Streams.wait("/w", 0, timeout: 1_000)
      :ok = Streams.cancel_wait("/w", pending)
      {:ok, _} = Streams.append("/w", "x")
      assert_receive {:slap_streams_wake, ^ref, :data}, 1_000
      assert {:ok, :data} = Streams.wait("/w", 0)

      assert {:ok, {:waiting, pending}} = Streams.wait("/w", :now)
      ref = Wait.ref(pending)
      {:ok, _} = Streams.close("/w")
      assert_receive {:slap_streams_wake, ^ref, :closed}, 1_000
      assert {:ok, :closed} = Streams.wait("/w", :now)

      {:ok, :created, _} = Streams.create("/w2")
      assert {:ok, {:waiting, pending}} = Streams.wait("/w2", 0)
      ref = Wait.ref(pending)
      :ok = Streams.delete("/w2")
      assert_receive {:slap_streams_wake, ^ref, :deleted}, 1_000
    end

    test "can be cancelled" do
      assert {:ok, {:waiting, pending}} = Streams.wait("/w", 0)
      ref = Wait.ref(pending)
      :ok = Streams.cancel_wait("/w", pending)
      {:ok, _} = Streams.append("/w", "x")
      refute_receive {:slap_streams_wake, ^ref, _}, 100
    end

    test "a read with :wait returns what comes during the wait" do
      waiting_read("/w", 0, wait: 5_000)
      {:ok, _} = Streams.append("/w", "x")
      assert_receive {:read, {:ok, %{messages: [{0, "x"}], next_offset: 5}}}, 1_000
    end

    test "a read with :wait that times out reads again: empty, from the tail" do
      {:ok, _} = Streams.append("/w", "x")

      assert {:ok, %{messages: [], next_offset: 5, closed: false}} =
               Streams.read("/w", :now, wait: 50)
    end

    test "an idle read uses its whole timeout and returns an empty page" do
      assert {:ok, %{messages: [], next_offset: 0, up_to_date: true}} =
               Streams.read("/w", 0, wait: 50, timeout: 50)
    end

    test "a read with :wait ends on a close, and on a delete" do
      waiting_read("/w", 0, wait: 5_000)
      {:ok, _} = Streams.close("/w")
      assert_receive {:read, {:ok, %{messages: [], next_offset: 0, closed: true}}}, 1_000

      {:ok, :created, _} = Streams.create("/w2")
      waiting_read("/w2", 0, wait: 5_000)
      :ok = Streams.delete("/w2")
      assert_receive {:read, {:error, :deleted}}, 1_000
    end

    test "a read with :wait does not wait when there is data, or at the end of a closed stream" do
      {:ok, _} = Streams.append("/w", "x")
      assert {:ok, %{messages: [{0, "x"}]}} = Streams.read("/w", 0, wait: 60_000)
      {:ok, _} = Streams.close("/w")
      assert {:ok, %{messages: [], closed: true}} = Streams.read("/w", 5, wait: 60_000)
    end
  end

  # Starts a read with `opts` in another process, and returns once it waits
  # (the stream server has answered its wait). The result comes as
  # `{:read, result}`.
  defp waiting_read(path, offset, opts) do
    test = self()

    reader =
      spawn_link(fn ->
        receive do
          :go -> send(test, {:read, Streams.read(path, offset, opts)})
        end
      end)

    :erlang.trace(reader, true, [:receive])
    send(reader, :go)
    assert_receive {:trace, ^reader, :receive, {_, {:ok, {:waiting, _, _}}}}, 1_000
    :erlang.trace(reader, false, [:receive])
  end

  describe "delete (§5.4)" do
    test "the stream is gone; the path can be created again, with a new stream id" do
      {:ok, :created, _} = Streams.create("/del", body: "old")
      {:ok, %{sid: old_sid}} = Streams.read_internal("/del", 0)
      assert :ok = Streams.delete("/del")
      assert {:error, :not_found} = Streams.delete("/del")
      assert {:error, :not_found} = Streams.head("/del")
      assert {:error, :not_found} = Streams.read("/del", 0)

      assert {:ok, :created, %{next_offset: 0}} = Streams.create("/del")
      assert {:ok, %{sid: new_sid, messages: []}} = Streams.read_internal("/del", 0)
      assert new_sid != old_sid
    end
  end

  describe "expiry" do
    test "Expires-At: the stream is gone once it passes" do
      at = System.system_time(:millisecond) + 300
      {:ok, :created, %{expires_at_ms: ^at}} = Streams.create("/exp", expires_at_ms: at)
      assert {:ok, _} = Streams.head("/exp")
      Process.sleep(400)
      assert {:error, :not_found} = Streams.head("/exp")
      assert {:error, :not_found} = Streams.append("/exp", "x")
      assert {:ok, :created, _} = Streams.create("/exp")
    end

    test "a sliding TTL is reset by reads and writes, not by head" do
      {:ok, :created, _} = Streams.create("/ttl", ttl_s: 1)

      for _ <- 1..3 do
        Process.sleep(600)
        assert {:ok, _} = Streams.read("/ttl", 0)
      end

      Process.sleep(600)
      {:ok, _} = Streams.append("/ttl", "keep")

      # Heads at 400 and 800 ms do not extend it, so it is gone by 1,200.
      for _ <- 1..2 do
        Process.sleep(400)
        assert {:ok, _} = Streams.head("/ttl")
      end

      Process.sleep(400)
      assert {:error, :not_found} = Streams.head("/ttl")
      assert {:error, :not_found} = Streams.read("/ttl", 0)
    end

    test "expiry survives a restart of the stream server" do
      {:ok, :created, _} = Streams.create("/ttl2", ttl_s: 1)
      Process.exit(server("/ttl2"), :kill)
      Process.sleep(1_100)
      assert {:error, :not_found} = Streams.head("/ttl2")
    end
  end

  describe "restarts" do
    @tag child_options: [idle_timeout: 100]
    test "an idle server stops, and the next request reloads the stream" do
      {:ok, :created, _} = Streams.create("/idle", content_type: "text/plain")
      {:ok, _} = Streams.append("/idle", "a", producer: {"w", 0, 0}, stream_seq: "1")
      pid = server("/idle")
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, :normal}, 1_000

      assert {:ok, %{result: :duplicate}} = Streams.append("/idle", "a", producer: {"w", 0, 0})
      assert {:error, :stream_seq_conflict} = Streams.append("/idle", "b", stream_seq: "1")
      assert {:ok, %{next_offset: 10}} = Streams.append("/idle", "b", producer: {"w", 0, 1})
    end

    test "streams survive the cluster restarting", %{cluster_opts: opts} do
      {:ok, :created, _} = Streams.create("/keep", content_type: "text/plain")
      {:ok, _} = Streams.append("/keep", "x", close: true)
      stop_supervised!(Streams.Cluster)
      start_supervised!({Streams.Cluster, opts})

      assert {:ok, %{next_offset: 5, closed: true, content_type: "text/plain"}} =
               Streams.head("/keep")

      assert {:ok, %{messages: [{0, "x"}], closed: true}} = Streams.read("/keep", 0)
    end
  end
end
