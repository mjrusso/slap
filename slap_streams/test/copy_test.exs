defmodule Slap.Streams.CopyTest do
  # Cluster registration and the fake clock are shared across tests.
  use Slap.Streams.Test.ClusterCase, async: false

  import Slap.Streams.Test.CreationFixtures

  alias Slap.SlateDB
  alias Slap.Streams.Creation.Target
  alias Slap.Streams.Retention
  alias Slap.Streams.Store.Meta
  alias Slap.Streams.Test.FakeClock

  @moduletag :capture_log

  test "copy publication wakes source repair while the target's repair job is suspended" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    source_ctx = ctx_for("/source")

    path = Enum.find(for(i <- 0..100, do: "/copy-#{i}"), &(ctx_for(&1).n != source_ctx.n))
    c = reserve(path, %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, {:pending, 5}} = call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5})
    repair_job = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, c.target.shard, :repair))
    :ok = :sys.suspend(repair_job)
    source = server("/source")
    :erlang.trace_pattern({Retention, :unregister, 4}, true, [:local])
    :erlang.trace(source, true, [:call, {:tracer, self()}])

    try do
      assert {:ok, {:completed, :created, _}} = call(c.target, {:activate, c.target})
      target = c.target
      assert_receive {:trace, ^source, :call, {Retention, :unregister, [_, _, _, ^target]}}, 5_000
      :sys.get_state(source)
      :ok = SlateDB.flush(source_ctx.db)
      assert registrations("/source") == []
      assert {:ok, %{messages: [{0, "a"}]}} = Streams.read(path, 0)
    after
      :erlang.trace(source, false, [:call])
      :erlang.trace_pattern({Retention, :unregister, 4}, false, [:local])
      :sys.resume(repair_job)
    end
  end

  test "a seal after copy reservation prevents publication and releases protection" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    c = reserve("/g/.copy", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, {:pending, 5}} = call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5})
    :ok = Streams.seal("/g")

    assert {:error, :sealed} = call(c.target, {:activate, c.target})
    repair_until(%{"/source" => 0, c.target.path => nil})
    assert Streams.head(c.target.path) == {:error, :not_found}
    assert registrations("/source") == []
  end

  test "cancellation fences delayed registration, page writes and activation across path reuse" do
    {:ok, :created, _} = Streams.create("/source", body: "old")
    c = reserve("/target", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, {page, next}} = Streams.retained(cap, 0, cap.offset, 1024, 5_000, Streams.Cluster)
    :ok = Streams.delete("/target")
    repair_until(%{"/source" => 0, "/target" => nil})
    {:ok, :created, _} = Streams.create("/target", body: "replacement")
    assert {:error, :conflict} = call(c.target, {:copy_page, c.target, 0, page, next})
    assert {:error, :conflict} = call(c.target, {:activate, c.target})
    assert :ok = call(c.source, {:unregister, c.source, c.target})
    assert {:ok, _late_cap} = call(c.source, {:register, Target.selection(c), nil})
    repair_until(%{"/source" => 0})
    assert registrations("/source") == []
    assert {:ok, %{messages: [{0, "replacement"}]}} = Streams.read("/target", 0)
  end

  test "manual copies preserve trim, capture, closure and expiry without producer or sequence state" do
    {:ok, :created, _} = Streams.create("/source", ttl_s: 100)
    {:ok, _} = Streams.append("/source", "a", producer: {"p", 0, 0}, stream_seq: "10")
    {:ok, _} = Streams.append("/source", "b", producer: {"p", 0, 1}, stream_seq: "11")
    :ok = Streams.trim("/source", 5)
    c = reserve("/copy", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, _} = Streams.append("/source", "later")
    :ok = Streams.delete("/source")
    assert {:ok, :created, %{next_offset: 10, ttl_s: 100}} = Streams.copy("/source", "/copy")
    assert Streams.read("/copy", 0) == {:error, :trimmed}
    assert {:ok, %{messages: [{5, "b"}]}} = Streams.read("/copy", 5)
    assert {:ok, _} = Streams.append("/copy", "new", producer: {"p", 0, 0}, stream_seq: "0")
    assert {:ok, :exists, _} = Streams.copy("/source", "/copy")
    assert {:error, :conflict} = Streams.copy("/source", "/copy", at: 5)
    repair_until(%{"/source" => nil})
    assert Meta.reference(meta("/copy")) == nil
    assert Streams.head("/source") == {:error, :not_found}
    {:ok, _} = Streams.close("/copy")
    {:ok, :created, %{closed: true}} = Streams.copy("/copy", "/closed")
  end

  test "completed copy retries compare public selection after internal request defaults change" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")
    {:ok, :created, _} = Streams.copy("/source", "/copy", at: 10)
    :ok = Streams.delete("/source")
    repair_until(%{"/source" => nil})
    kill(server("/copy"))

    request =
      Target.request(%{forked_from: "/source", fork_offset: 10})
      |> Map.put(:future_default, false)
      |> Map.put(:content_type, "application/octet-stream")

    assert {:ok, {:completed, :exists, %{next_offset: 10}}} =
             Streams.internal("/copy", {:reservation, :copy, request})

    assert {:error, :conflict} = Streams.copy("/source", "/copy")
    assert {:error, :conflict} = Streams.copy("/source", "/copy", at: 5)
    assert {:error, :conflict} = Streams.copy("/other", "/copy", at: 10)

    assert {:error, :conflict} =
             Streams.copy("/source", "/copy", at: 10, source_placement_key: "other-route")
  end

  test "pending copy retries keep the saved capture when internal defaults change" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")
    c = reserve("/copy", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, captured}} = call(c.target, {:capture, c.target, cap})
    kill(server("/copy"))
    request = c.request |> Map.put(:future_default, false) |> Map.put(:content_type, "text/plain")

    assert {:ok, {:pending, ^captured}} =
             Streams.internal("/copy", {:reservation, :copy, request})

    {:ok, _} = Streams.append("/source", "later")
    :ok = Streams.delete("/source")
    assert {:ok, :created, %{next_offset: 10}} = Streams.copy("/source", "/copy")
    assert {:ok, %{messages: [{0, "prefix"}]}} = Streams.read("/copy", 0)
  end

  test "a copied relative TTL starts when the captured copy activates" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    {:ok, :created, _} = Streams.create("/source", body: "x", ttl_s: 30)
    c = reserve("/copy", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    FakeClock.advance(20_000)
    assert {:ok, :created, %{ttl_s: 30}} = Streams.copy("/source", "/copy")
    repair_until(%{"/source" => 0})
    FakeClock.advance(15_000)
    assert Streams.head("/source") == {:error, :not_found}
    assert {:ok, _} = Streams.head("/copy")
  end

  test "copies preserve pagination at nonboundary offsets, including subsequent own appends" do
    {:ok, :created, _} = Streams.create("/a", body: "hello")
    {:ok, :created, _} = Streams.create("/b", forked_from: "/a", fork_offset: 3, body: "own")
    {:ok, :created, _} = Streams.copy("/b", "/copy")
    {:ok, :created, _} = Streams.copy("/a", "/prefix", at: 3)
    {:ok, _} = Streams.append("/prefix", "own")
    :ok = Streams.delete("/b")
    :ok = Streams.delete("/a")
    repair_until(%{"/a" => nil, "/b" => nil})

    for path <- ["/copy", "/prefix"] do
      assert {:ok, %{messages: [{0, "hello"}], next_offset: 3}} =
               Streams.read(path, 0, max_bytes: 5)

      assert {:ok, %{messages: [{3, "own"}], next_offset: 10}} =
               Streams.read(path, 3, max_bytes: 5)
    end
  end

  test "explicit copy offsets preserve trimmed, empty and closed captured ranges" do
    {:ok, :created, _} =
      Streams.create("/source", body: "[1,2,3]", content_type: "application/json", closed: true)

    :ok = Streams.trim("/source", 5)
    {:ok, :created, %{next_offset: 10, closed: true}} = Streams.copy("/source", "/at10", at: 10)
    assert Streams.read("/at10", 0) == {:error, :trimmed}
    assert {:ok, %{messages: [{5, "2"}], closed: true}} = Streams.read("/at10", :start)
    {:ok, :created, %{next_offset: 0, closed: true}} = Streams.copy("/source", "/empty", at: 0)
    assert {:ok, %{messages: [], next_offset: 0, closed: true}} = Streams.read("/empty", :start)

    assert {:error, {:bad_request, :fork_offset_beyond_source}} =
             Streams.copy("/source", "/beyond", at: 16)

    {:ok, :created, _} = Streams.create("/open", body: "x")
    c = reserve("/captured-open", %{forked_from: "/open"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, _} = Streams.close("/open")
    assert {:ok, :created, %{closed: false}} = Streams.copy("/open", "/captured-open")
  end

  test "completed fork and copy creates retain the target's sliding TTL access semantics" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    {:ok, :created, _} = Streams.create("/source", body: "x", ttl_s: 100)
    opts = [forked_from: "/source", ttl_s: 1]
    {:ok, :created, _} = Streams.create("/fork", opts)
    {:ok, :created, _} = Streams.copy("/fork", "/copy")
    repair_until(%{"/source" => 1, "/fork" => 0})
    FakeClock.advance(500)
    assert {:ok, :exists, _} = Streams.create("/fork", opts)
    assert {:ok, :exists, _} = Streams.copy("/fork", "/copy")
    FakeClock.advance(600)
    assert {:ok, _} = Streams.head("/fork")
    assert {:ok, _} = Streams.head("/copy")
    FakeClock.advance(500)
    assert Streams.head("/fork") == {:error, :not_found}
    assert Streams.head("/copy") == {:error, :not_found}
  end

  test "delayed source operations cannot touch a replacement source incarnation" do
    {:ok, :created, _} = Streams.create("/source", body: "old")
    c = reserve("/target", %{forked_from: "/source"}, :copy)
    register(c)
    :ok = Streams.delete("/target")
    repair_until(%{"/source" => 0, "/target" => nil})
    :ok = Streams.delete("/source")
    {:ok, :created, _} = Streams.create("/source", body: "new")
    {:ok, :created, _} = Streams.create("/new-fork", forked_from: "/source")
    assert {:error, :source_gone} = call(c.source, {:register, Target.selection(c), nil})
    assert :ok = call(c.source, {:unregister, c.source, c.target})
    assert [_] = registrations("/source")
    assert {:ok, %{messages: [{0, "new"}]}} = Streams.read("/new-fork", 0)
  end

  test "a copy resumes persisted page progress after source removal without recapturing its range" do
    {:ok, :created, _} = Streams.create("/source", body: "one")
    for body <- ["two", "three"], do: {:ok, _} = Streams.append("/source", body)

    target =
      Enum.find(for(i <- 1..100, do: "/copy-#{i}"), &(ctx_for(&1).n != ctx_for("/source").n))

    c = reserve(target, %{forked_from: "/source"}, :copy)
    cap = register(c)
    assert cap.source.shard != cap.target.shard
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    {:ok, {page, next}} = Streams.retained(cap, 0, cap.offset, 3, 5_000, Streams.Cluster)
    assert next < cap.offset
    {:ok, {:pending, ^next}} = call(c.target, {:copy_page, c.target, 0, page, next})
    assert {:ok, {:pending, ^next}} = call(c.target, {:copy_page, c.target, 0, page, next})
    kill(server(target))
    {:ok, _} = Streams.append("/source", "later")
    :ok = Streams.trim("/source", cap.offset)
    :ok = Streams.delete("/source")
    assert {:ok, :created, %{next_offset: 23}} = Streams.copy("/source", target)
    repair_until(%{"/source" => nil})
    assert Streams.head("/source") == {:error, :not_found}

    assert {:ok, {:completed, :exists, %{next_offset: 23}}} =
             call(c.target, {:copy_page, c.target, 0, page, next})

    assert {:ok, %{messages: [{0, "one"}, {7, "two"}, {14, "three"}]}} = Streams.read(target, 0)
  end

  test "delayed source replies reconcile completion by the original target incarnation" do
    for outcome <- [:captured, :source_not_found, :unavailable, :replaced] do
      source = "/source-#{outcome}"
      target = "/target-#{outcome}"
      {:ok, :created, _} = Streams.create(source, body: "prefix")
      c = reserve(target, %{forked_from: source}, :copy)
      cap = register(c)
      {:ok, _} = Streams.append(source, "later")
      reply = if outcome == :captured, do: {:ok, cap}, else: {:error, :source_not_found}
      reply = if outcome == :unavailable, do: {:error, :unavailable}, else: reply

      result =
        during_requests(
          c.source,
          [{{:register, Target.selection(c), nil}, reply}],
          fn -> Streams.copy(source, target) end,
          fn -> finish_competing_copy(c, outcome == :replaced) end
        )

      if outcome == :replaced do
        assert result == {:error, :source_not_found}
        assert {:ok, %{messages: [{0, "replacement"}]}} = Streams.read(target, 0)
      else
        assert {:ok, :exists, %{next_offset: 10}} = result
        assert {:ok, %{messages: [{0, "prefix"}]}} = Streams.read(target, 0)
      end
    end
  end

  test "a delayed source rejection preserves a matching copy's captured range and progress" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    c = reserve("/target", %{forked_from: "/source", fork_offset: 10}, :copy)
    request = {:register, Target.selection(c), nil}

    assert {:error, {:bad_request, :fork_offset_beyond_source}} =
             rejection = call(c.source, request)

    assert ^rejection =
             during_requests(
               c.source,
               [{request, rejection}],
               fn -> Streams.copy("/source", "/target", at: 10) end,
               fn ->
                 {:ok, _} = Streams.append("/source", "b")
                 cap = register(c)
                 {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
                 {:ok, {:pending, 5}} = call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5})
               end
             )

    assert meta("/target").sid == c.target.sid
    assert meta("/target").creation.progress.cursor == 5
    assert Streams.head("/target") == {:error, :unavailable}
    {:ok, _} = Streams.append("/source", "later")
    :ok = Streams.trim("/source", 10)
    :ok = Streams.delete("/source")
    kill(server("/target"))

    assert {:ok, :created, %{next_offset: 10}} = Streams.copy("/source", "/target", at: 10)
    assert {:ok, %{messages: [{0, "a"}, {5, "b"}]}} = Streams.read("/target", 0)
    repair_until(%{"/source" => nil})
  end

  test "a failed retained read reconciles publication without accepting a replacement copy" do
    for replaced? <- [false, true] do
      source = "/source-#{replaced?}"
      target = "/target-#{replaced?}"
      {:ok, :created, _} = Streams.create(source, body: "prefix")
      c = reserve(target, %{forked_from: source}, :copy)
      cap = register(c)
      {:ok, {:pending, captured}} = call(c.target, {:capture, c.target, cap})
      request = {:reservation, :copy, Target.request(%{forked_from: source})}

      result =
        during_requests(
          c.target,
          [
            {request, {:ok, {:pending, captured}}},
            {{:capture, c.target, cap}, {:ok, {:pending, captured}}}
          ],
          fn -> Streams.copy(source, target) end,
          fn -> finish_competing_copy(c, replaced?) end
        )

      if replaced? do
        assert result == {:error, :unavailable}
        assert {:ok, %{messages: [{0, "replacement"}]}} = Streams.read(target, 0)
      else
        assert {:ok, :exists, %{next_offset: 10}} = result
        assert {:ok, %{messages: [{0, "prefix"}]}} = Streams.read(target, 0)
      end
    end
  end

  test "manual copies reject expired activation and validate before reserving" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)

    {:ok, :created, _} =
      Streams.create("/source", body: "x", expires_at_ms: FakeClock.now_ms() + 100)

    c = reserve("/target", %{forked_from: "/source"}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    FakeClock.advance(200)
    assert {:error, :source_gone} = Streams.copy("/source", "/target")
    repair_until(%{"/target" => nil})
    assert Streams.head("/target") == {:error, :not_found}
    assert_raise ArgumentError, fn -> Streams.copy("/source", "/invalid", timeout: -1) end
    assert_raise ArgumentError, fn -> Streams.copy("/source", "/invalid", unknown: true) end
    assert {:error, {:bad_request, :invalid_path}} = Streams.copy(nil, "/invalid")
    assert {:error, {:bad_request, :invalid_offset}} = Streams.copy("/source", "/invalid", at: -1)
    assert Streams.head("/invalid") == {:error, :not_found}
  end

  defp finish_competing_copy(c, replaced?) do
    source = c.source.path
    target = c.target.path
    assert {:ok, :created, %{next_offset: 10}} = Streams.copy(source, target)
    assert :ok = Streams.delete(source)
    repair_until(%{source => nil})

    if replaced? do
      assert :ok = Streams.delete(target)
      assert {:ok, :created, _} = Streams.create(source, body: "replacement")
      assert {:ok, :created, _} = Streams.copy(source, target)
    end
  end
end
