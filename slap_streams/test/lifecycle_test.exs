defmodule Slap.Streams.LifecycleTest do
  use Slap.Streams.Test.ClusterCase, async: false

  @moduletag :capture_log
  @moduletag child_options: [deleter_page: 5]

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Jobs.{Deleter, Expiry, Repair}
  alias Slap.Streams.Store.{Batch, Keys, Read}
  alias Slap.Streams.Test.FakeClock

  describe "forks (§4.2)" do
    test "copy the source up to the fork offset, then diverge" do
      {:ok, :created, _} = Streams.create("/src", content_type: "text/plain")
      for m <- ["a", "b", "c"], do: {:ok, _} = Streams.append("/src", m)

      # "a" is at 0, "b" at 5, "c" at 10.
      assert {:ok, :created, %{next_offset: 10, content_type: "text/plain"}} =
               Streams.create("/fork", forked_from: "/src", fork_offset: 10)

      assert {:ok, read} = Streams.read("/fork", 0)
      assert bodies(read) == ["a", "b"]

      {:ok, _} = Streams.append("/fork", "x")
      assert {:ok, read} = Streams.read("/fork", 10)
      assert bodies(read) == ["x"]
      assert {:ok, read} = Streams.read("/src", 10)
      assert bodies(read) == ["c"]

      # Without an offset, the fork starts at the source's tail.
      assert {:ok, :created, %{next_offset: 15}} = Streams.create("/fork2", forked_from: "/src")
      # As in the official server, a retry repeats the content type.
      assert {:ok, :exists, _} =
               Streams.create("/fork2", forked_from: "/src", content_type: "text/plain")

      assert {:error, :conflict} = Streams.create("/fork2", forked_from: "/src", fork_offset: 5)
      assert {:error, :conflict} = Streams.create("/fork2")
    end

    test "errors" do
      {:ok, :created, _} = Streams.create("/src", content_type: "text/plain", body: "hello")

      assert {:error, :source_not_found} = Streams.create("/f", forked_from: "/nope")

      assert {:error, :content_type_mismatch} =
               Streams.create("/f", forked_from: "/src", content_type: "application/json")

      assert {:error, {:bad_request, :fork_offset_beyond_source}} =
               Streams.create("/f", forked_from: "/src", fork_offset: 10)

      assert {:error, {:bad_request, :fork_of_itself}} =
               Streams.create("/src2", forked_from: "/src2")

      assert Streams.head("/f") == {:error, :not_found}
    end

    test "sub-offsets: JSON messages, or a prefix of a binary message" do
      json = [content_type: "application/json"]
      {:ok, :created, _} = Streams.create("/j", [body: "[1, 2, 3]"] ++ json)

      # Each message is 4 + 1 bytes.
      assert {:ok, :created, %{next_offset: 10}} =
               Streams.create("/jf", forked_from: "/j", fork_offset: 0, fork_sub_offset: 2)

      assert {:ok, read} = Streams.read("/jf", 0)
      assert bodies(read) == ["1", "2"]

      assert {:error, {:bad_request, :invalid_fork_sub_offset}} =
               Streams.create("/jf2", forked_from: "/j", fork_offset: 0, fork_sub_offset: 4)

      {:ok, :created, _} = Streams.create("/b", body: "hello")

      assert {:ok, :created, %{next_offset: 7}} =
               Streams.create("/bf", forked_from: "/b", fork_offset: 0, fork_sub_offset: 3)

      assert {:ok, read} = Streams.read("/bf", 0)
      assert bodies(read) == ["hel"]
    end

    @tag child_options: [deleter_page: 5, max_fork_copy_bytes: 8]
    test "the copy is capped by :max_fork_copy_bytes" do
      {:ok, :created, _} = Streams.create("/src", body: "0123456789")
      assert {:error, :payload_too_large} = Streams.create("/f", forked_from: "/src")
      assert {:ok, :created, _} = Streams.create("/f", forked_from: "/src", fork_offset: 0)
    end

    test "expiry: the fork's own, else the source's" do
      {:ok, :created, _} = Streams.create("/src", ttl_s: 60)
      assert {:ok, :created, %{ttl_s: 60}} = Streams.create("/f1", forked_from: "/src")

      assert {:ok, :created, %{ttl_s: nil, expires_at_ms: 4_102_444_800_000}} =
               Streams.create("/f2", forked_from: "/src", expires_at_ms: 4_102_444_800_000)
    end

    test "a source with forks is soft-deleted, and goes with its last fork" do
      {:ok, :created, _} = Streams.create("/src", body: "data")
      {:ok, :created, _} = Streams.create("/f1", forked_from: "/src")
      {:ok, :created, _} = Streams.create("/f2", forked_from: "/src")

      assert :ok = Streams.delete("/src")
      assert Streams.head("/src") == {:error, :gone}
      assert Streams.read("/src", 0) == {:error, :gone}
      assert Streams.delete("/src") == {:error, :gone}
      assert Streams.create("/src") == {:error, :conflict}
      assert Streams.create("/f3", forked_from: "/src") == {:error, :source_gone}

      assert {:ok, read} = Streams.read("/f1", 0)
      assert bodies(read) == ["data"]

      :ok = Streams.delete("/f1")
      assert Streams.head("/src") == {:error, :gone}
      :ok = Streams.delete("/f2")
      assert Streams.head("/src") == {:error, :not_found}
      assert {:ok, :created, _} = Streams.create("/src")
    end

    test "removal cascades up a chain of soft-deleted sources" do
      {:ok, :created, _} = Streams.create("/a", body: "x")
      {:ok, :created, _} = Streams.create("/b", forked_from: "/a")
      {:ok, :created, _} = Streams.create("/c", forked_from: "/b")

      :ok = Streams.delete("/a")
      :ok = Streams.delete("/b")
      assert Streams.head("/a") == {:error, :gone}
      assert Streams.head("/b") == {:error, :gone}

      :ok = Streams.delete("/c")
      for p <- ["/a", "/b", "/c"], do: assert(Streams.head(p) == {:error, :not_found})
      assert_all_deleted()
    end

    test "an interrupted copy is finished by a retried create" do
      {:ok, :created, _} = Streams.create("/src", content_type: "text/plain")
      for m <- ["a", "b", "c"], do: {:ok, _} = Streams.append("/src", m)
      opts = [forked_from: "/src", content_type: "text/plain", body: "d"]
      {:ok, :created, _} = Streams.create("/f", opts)

      # As if the fork's server crashed during the copy.
      ctx = ctx_for("/f")
      {:ok, meta} = Read.get_meta(ctx.db, "/f")
      {:ok, _} = SlateDB.write(ctx.db, Batch.put_meta("/f", %{meta | copying: true}))
      {:ok, _} = SlateDB.write(ctx.db, [{:delete, Keys.msg(meta.sid, 5, 0)}])
      kill(server("/f"))

      assert Streams.head("/f") == {:error, :unavailable}
      assert Streams.read("/f", 0) == {:error, :unavailable}
      assert Streams.append("/f", "e") == {:error, :unavailable}

      assert {:ok, :created, %{next_offset: 20}} = Streams.create("/f", opts)
      assert {:ok, read} = Streams.read("/f", 0)
      assert bodies(read) == ["a", "b", "c", "d"]
    end
  end

  describe "forks: failures (review fixes)" do
    test "a retried create resumes a copy even when the fork inherited its expiry" do
      {:ok, :created, _} = Streams.create("/src", ttl_s: 3600)
      {:ok, _} = Streams.append("/src", "a")
      opts = [forked_from: "/src"]
      {:ok, :created, _} = Streams.create("/f", opts)

      ctx = ctx_for("/f")
      {:ok, meta} = Read.get_meta(ctx.db, "/f")
      # Inherited from the source, not asked for.
      assert meta.ttl_s == 3600
      {:ok, _} = SlateDB.write(ctx.db, Batch.put_meta("/f", %{meta | copying: true}))
      kill(server("/f"))

      assert {:ok, :created, _} = Streams.create("/f", opts)
      assert {:ok, read} = Streams.read("/f", 0)
      assert bodies(read) == ["a"]
    end

    test "a copy whose source was deleted meanwhile is abandoned, not left stuck" do
      {:ok, :created, _} = Streams.create("/src")
      {:ok, _} = Streams.append("/src", "a")
      {:ok, :created, _} = Streams.create("/f", forked_from: "/src")

      # As if the copy were interrupted, and then the source deleted.
      ctx = ctx_for("/f")
      {:ok, meta} = Read.get_meta(ctx.db, "/f")
      {:ok, _} = SlateDB.write(ctx.db, Batch.put_meta("/f", %{meta | copying: true}))
      kill(server("/f"))
      :ok = Streams.delete("/src")

      # The retry gets a definite answer, and both paths are free again.
      assert {:error, :source_gone} = Streams.create("/f", forked_from: "/src")
      assert {:error, :not_found} = Streams.head("/f")
      wait_until(fn -> match?({:error, :not_found}, Streams.head("/src")) end)
      assert_all_deleted()
    end

    test "a fork that could not unregister does not keep its deleted source forever" do
      {:ok, :created, _} = Streams.create("/src")
      # Registered, but never created (as if its delete could not reach the
      # source to unregister).
      {:ok, _} = Streams.internal("/src", {:fork_source, "/ghost", []})
      :ok = Streams.delete("/src")
      assert {:error, :gone} = Streams.head("/src")

      # The next create finds the stale fork, and the source goes.
      assert {:error, :conflict} = Streams.create("/src")
      wait_until(fn -> match?({:ok, :created, _}, Streams.create("/src")) end)
    end
  end

  describe "fork checks" do
    test "a fork that does not answer does not hold up its source's other forks" do
      {:ok, :created, _} = Streams.create("/src")
      {:ok, _} = Streams.internal("/src", {:fork_source, "/ghost", []})
      {:ok, :created, _} = Streams.create("/slow", forked_from: "/src")
      :ok = Streams.delete("/src")
      assert meta("/src").forks == ["/slow", "/ghost"]

      # A refused create asks for the forks to be checked; "/slow" is first.
      :ok = :sys.suspend(server("/slow"))
      assert {:error, :conflict} = Streams.create("/src")
      wait_until(fn -> meta("/src").forks == ["/slow"] end)

      :ok = :sys.resume(server("/slow"))
      :ok = Streams.delete("/slow")
      assert Streams.head("/src") == {:error, :not_found}
    end
  end

  describe "repair sweep" do
    test "removes a fork left copying past the grace period, and unregisters it", context do
      {:ok, :created, _} = Streams.create("/src")
      {:ok, _} = Streams.append("/src", "a")
      {:ok, :created, _} = Streams.create("/f", forked_from: "/src", body: "b")

      # As if the fork's server crashed during the copy, and nobody retried.
      ctx = ctx_for("/f")
      {:ok, meta} = Read.get_meta(ctx.db, "/f")
      stuck = Batch.put_meta("/f", %{meta | copying: true}) ++ Batch.repair("/f", meta.sid)
      {:ok, _} = SlateDB.write(ctx.db, stuck)
      kill(server("/f"))

      sweep_all()
      assert Streams.head("/f") == {:error, :unavailable}

      stop_supervised!(Streams.Cluster)

      start_supervised!(
        {Streams.Cluster,
         Keyword.put(context.cluster_opts, :child_options, deleter_page: 5, fork_copy_grace: 0)}
      )

      sweep_all()
      assert Streams.head("/f") == {:error, :not_found}

      # Unregistered: the source is deleted outright, not soft-deleted.
      :ok = Streams.delete("/src")
      assert Streams.head("/src") == {:error, :not_found}
      assert_all_deleted()
    end

    test "removes a soft-deleted source whose fork could not unregister" do
      {:ok, :created, _} = Streams.create("/src")
      {:ok, _} = Streams.internal("/src", {:fork_source, "/ghost", []})
      :ok = Streams.delete("/src")
      assert {:error, :gone} = Streams.head("/src")

      sweep_all()
      assert Streams.head("/src") == {:error, :not_found}
      assert_all_deleted()
    end

    test "drops nothing a live fork or soft-deleted source still needs" do
      {:ok, :created, _} = Streams.create("/src")
      {:ok, :created, _} = Streams.create("/f", forked_from: "/src")
      :ok = Streams.delete("/src")

      sweep_all()
      assert {:error, :gone} = Streams.head("/src")
      assert rows(0x08) == 1

      :ok = Streams.delete("/f")
      assert Streams.head("/src") == {:error, :not_found}
      assert_all_deleted()
    end
  end

  describe "expiry sweep" do
    setup do
      FakeClock.install(1_700_000_000_000)
      on_exit(&FakeClock.uninstall/0)
    end

    test "deletes expired streams that nobody accesses" do
      {:ok, :created, _} = Streams.create("/t", ttl_s: 1)
      {:ok, :created, _} = Streams.create("/e", expires_at_ms: FakeClock.now_ms() + 500)
      {:ok, :created, _} = Streams.create("/keep", ttl_s: 3600)
      FakeClock.advance(2000)

      sweep()
      assert meta("/t") == nil
      assert meta("/e") == nil
      assert meta("/keep") != nil
      assert rows(0x05) == 1
    end

    test "a sliding TTL keeps its last access across a server stop" do
      {:ok, :created, _} = Streams.create("/s", ttl_s: 100)
      # Less than a tenth of the TTL: the saved deadline does not move yet.
      FakeClock.advance(5_000)
      {:ok, _} = Streams.read("/s", 0)
      :ok = GenServer.stop(server("/s"))

      # 101 s after the create, 96 s after the read: still alive.
      FakeClock.advance(96_000)
      assert {:ok, _} = Streams.head("/s")
      FakeClock.advance(5_000)
      assert {:error, :not_found} = Streams.head("/s")
    end

    test "a stream server that does not answer does not hold up the others" do
      shard = Streams.Cluster.shard_for("/slow")
      other = Enum.find(for(i <- 0..200, do: "/e#{i}"), &(Streams.Cluster.shard_for(&1) == shard))
      {:ok, :created, _} = Streams.create("/slow", expires_at_ms: FakeClock.now_ms() + 100)
      {:ok, :created, _} = Streams.create(other, expires_at_ms: FakeClock.now_ms() + 200)
      FakeClock.advance(1000)

      # "/slow" is due first.
      :ok = :sys.suspend(server("/slow"))
      sweep = Task.async(fn -> Expiry.sweep(ctx_for("/slow")) end)
      wait_until(fn -> meta(other) == nil end)

      :ok = :sys.resume(server("/slow"))
      assert Task.await(sweep) == 2
      assert meta("/slow") == nil
    end

    test "a sliding TTL moves its index entry when accessed" do
      {:ok, :created, _} = Streams.create("/s", ttl_s: 10)
      FakeClock.advance(5000)
      {:ok, _} = Streams.read("/s", 0)

      # 11 s after the create, 6 s after the read.
      FakeClock.advance(6000)
      sweep()
      assert meta("/s") != nil

      FakeClock.advance(4000)
      sweep()
      assert meta("/s") == nil
      assert rows(0x05) == 0
    end
  end

  describe "trim" do
    test "reads before the trim point get :trimmed, and the deleter removes the data" do
      {:ok, :created, _} = Streams.create("/t", content_type: "text/plain")
      for m <- ["a", "b", "c"], do: {:ok, _} = Streams.append("/t", m)

      assert {:error, {:bad_request, _}} = Streams.trim("/t", 100)
      assert :ok = Streams.trim("/t", 10)
      assert :ok = Streams.trim("/t", 5)
      assert Streams.read("/t", 5) == {:error, :trimmed}
      assert {:ok, read} = Streams.read("/t", 10)
      assert bodies(read) == ["c"]

      ctx = ctx_for("/t")
      Deleter.drain(ctx)
      {:ok, %{sid: sid}} = Read.get_meta(ctx.db, "/t")
      assert [{key, _}] = SlateDB.scan(ctx.db, prefix: <<0x04, sid::64>>) |> Enum.to_list()
      assert Keys.decode_msg(key) == {10, 0}

      kill(server("/t"))
      assert Streams.read("/t", 0) == {:error, :trimmed}
    end
  end

  describe "deleter" do
    @tag settings: %{flush_interval: "1h"}
    test "acts on a delete only once it is durable" do
      ctx = ctx_for("/nd")
      create = Task.async(fn -> Streams.create("/nd", body: "x") end)
      Process.sleep(100)
      :ok = SlateDB.flush(ctx.db)
      {:ok, :created, _} = Task.await(create)
      {:ok, %{sid: sid}} = Read.get_meta(ctx.db, "/nd")

      # The delete is written but not durable (it could still be lost): the
      # deleter leaves the rows alone.
      delete = Task.async(fn -> Streams.delete("/nd") end)
      Process.sleep(100)
      deleter = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter))
      send(deleter, :tick)
      :sys.get_state(deleter)
      assert ctx.db |> SlateDB.scan(prefix: <<0x04, sid::64>>) |> Enum.count() == 1

      :ok = SlateDB.flush(ctx.db)
      assert :ok = Task.await(delete)
      assert_all_deleted()
    end

    test "deletes every row of a deleted stream" do
      {:ok, :created, _} = Streams.create("/d", body: "x")
      {:ok, _} = Streams.append("/d", "y", producer: {"p", 0, 0})
      :ok = Streams.trim("/d", 5)
      :ok = Streams.delete("/d")
      assert_all_deleted()
    end

    test "resumes from its cursor after a crash" do
      body = "[" <> Enum.map_join(1..3000, ",", &Integer.to_string/1) <> "]"
      {:ok, :created, _} = Streams.create("/big", content_type: "application/json", body: body)
      ctx = ctx_for("/big")
      {:ok, %{sid: sid}} = Read.get_meta(ctx.db, "/big")

      :ok = Streams.delete("/big")
      cursor = wait_for_cursor(ctx.db, sid)
      deleter = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter))
      kill(deleter)

      # Everything before the stored cursor is gone, and the rest is there.
      {:ok, <<stored::64>>} = SlateDB.get(ctx.db, Keys.delete_pending(sid))
      assert stored >= cursor

      offsets =
        ctx.db
        |> SlateDB.scan(prefix: <<0x04, sid::64>>)
        |> Enum.map(fn {k, _} -> elem(Keys.decode_msg(k), 0) end)

      assert offsets != []
      assert Enum.min(offsets) >= stored

      # The supervisor restarts it, and it finishes from the cursor.
      wait_until(fn -> GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter)) end)
      assert_all_deleted()
    end
  end

  # -- helpers --

  defp sweep, do: for(ctx <- shards(), do: Expiry.sweep(ctx))

  defp meta(path) do
    {:ok, meta} = Read.get_meta(ctx_for(path).db, path)
    meta
  end

  defp rows(type) do
    shards()
    |> Enum.map(fn ctx -> ctx.db |> SlateDB.scan(prefix: <<type>>) |> Enum.count() end)
    |> Enum.sum()
  end

  # After the deleters run, only the stream id counter is left.
  defp assert_all_deleted do
    for ctx <- shards() do
      Deleter.drain(ctx)
      assert ctx.db |> SlateDB.scan(gte: <<1>>) |> Enum.to_list() == []
    end
  end

  defp sweep_all, do: Enum.each(shards(), &Repair.sweep/1)

  defp shards do
    for n <- Streams.Cluster.local_shards() do
      {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
      ctx
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
  end

  # The deleter's cursor for `sid`, once it has moved.
  defp wait_for_cursor(db, sid) do
    case SlateDB.get(db, Keys.delete_pending(sid)) do
      {:ok, <<0::64>>} -> wait_for_cursor(db, sid)
      {:ok, <<cursor::64>>} -> cursor
      {:ok, nil} -> flunk("the deleter finished before it could be stopped")
    end
  end

  defp wait_until(fun, tries \\ 100) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("timed out")
      true -> Process.sleep(10) && wait_until(fun, tries - 1)
    end
  end
end
