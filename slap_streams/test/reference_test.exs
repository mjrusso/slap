defmodule Slap.Streams.ReferenceTest do
  # Cluster registration and the fake clock are shared across tests.
  use Slap.Streams.Test.ClusterCase, async: false

  import Slap.Streams.Test.CreationFixtures

  alias Slap.SlateDB
  alias Slap.Streams.{Reader, Retention, Stream, StreamServer, Wait}

  alias Slap.Streams.Creation.Target
  alias Slap.Streams.HTTP.Router
  alias Slap.Streams.Jobs.{Deleter, Expiry, Repair}
  alias Slap.Streams.Store.{Codec, Keys, Meta, Read}
  alias Slap.Streams.Test.FakeClock

  @moduletag :capture_log

  test "published reference retries preserve source routing across target reload" do
    source = "/source"
    first_key = Streams.placement_key(source)

    second_key =
      Enum.find(
        for(i <- 1..100, do: "other-source-#{i}"),
        &(Streams.Cluster.shard_for(&1) != Streams.Cluster.shard_for(first_key))
      )

    {:ok, :created, _} = Streams.create(source, body: "first")
    {:ok, :created, _} = Streams.create(source, body: "second", placement_key: second_key)

    for {target, key, bytes} <- [
          {"/first", first_key, "first"},
          {"/second", second_key, "second"}
        ] do
      opts = [forked_from: source, source_placement_key: key]
      {:ok, :created, _} = Streams.create(target, opts)
      assert {:ok, :exists, _} = Streams.create(target, opts)
      kill(server(target))
      assert {:ok, :exists, _} = Streams.create(target, opts)
      other_key = if key == first_key, do: second_key, else: first_key

      assert {:error, :conflict} =
               Streams.create(target, Keyword.put(opts, :source_placement_key, other_key))

      assert {:ok, %{messages: [{0, ^bytes}]}} = Streams.read(target, 0)
    end

    assert {:ok, :exists, _} = Streams.create("/first", forked_from: source)
    assert {:error, :conflict} = Streams.create("/second", forked_from: source)
  end

  test "published retries retain the original sub-offset selection after source removal" do
    {:ok, :created, _} =
      Streams.create("/source", content_type: "application/json", body: "[1,2,3]")

    opts = [
      forked_from: "/source",
      content_type: "application/json",
      fork_offset: 5,
      fork_sub_offset: 1,
      body: "[4]"
    ]

    {:ok, :created, _} = Streams.create("/fork", opts)
    sid = meta("/fork").sid
    :ok = Streams.delete("/source")
    kill(server("/fork"))
    assert {:ok, :exists, _} = Streams.create("/fork", opts)
    assert {:ok, :exists, _} = Streams.create("/fork", Keyword.delete(opts, :fork_offset))
    assert {:error, :conflict} = Streams.create("/fork", Keyword.put(opts, :fork_offset, 10))
    assert {:error, :conflict} = Streams.create("/fork", Keyword.put(opts, :fork_sub_offset, 0))

    assert {:error, :conflict} =
             Streams.create("/fork", Keyword.put(opts, :forked_from, "/other"))

    assert {:error, :conflict} = Streams.create("/fork", content_type: "application/json")
    assert meta("/fork").sid == sid
    assert {:ok, page} = Streams.read("/fork", 0)
    assert bodies(page) == ["1", "2", "4"]
  end

  test "reference publication uses one activation without a target capture write" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")
    req = %{forked_from: "/source", body: "own"}
    c = reserve("/target", req)
    cap = register(c)
    pid = server("/target")
    :erlang.trace(pid, true, [:receive, {:tracer, self()}])

    try do
      assert {:ok, :created, _} = Streams.create("/target", Map.to_list(req))
      target = c.target
      assert_receive {:trace, ^pid, :receive, {:"$gen_call", _, {:activate, ^target, ^cap}}}
      barrier = :erlang.trace_delivered(pid)
      assert_receive {:trace_delivered, ^pid, ^barrier}
      refute_received {:trace, ^pid, :receive, {:"$gen_call", _, {:capture, _, _}}}
      assert {:ok, %{messages: [{0, "prefix"}, {10, "own"}]}} = Streams.read("/target", 0)
    after
      :erlang.trace(pid, false, [:receive])
    end
  end

  test "registration read failures preserve a shared capture and return HTTP 503" do
    {:ok, :created, _} = Streams.create("/v1/stream/source", body: "prefix")

    req = %{
      forked_from: "/v1/stream/source",
      content_type: "application/octet-stream",
      body: "own"
    }

    c = reserve("/v1/stream/target", req)
    cap = register(c)
    {:ok, closed_db} = SlateDB.open("failed-registration-read", store: :memory)
    :ok = SlateDB.close(closed_db)
    source = server("/v1/stream/source")
    original = :sys.get_state(source).ctx

    :sys.replace_state(source, fn state ->
      %{state | ctx: %{state.ctx | db: closed_db}}
    end)

    try do
      result = Streams.create("/v1/stream/target", Map.to_list(req))

      conn =
        Plug.Test.conn(:put, "/v1/stream/target", "own")
        |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
        |> Plug.Conn.put_req_header("stream-forked-from", "/v1/stream/source")
        |> Router.call(Router.init([]))

      assert Streams.head("/v1/stream/target") == {:error, :unavailable}
      assert result == {:error, :unavailable}
      assert conn.status == 503
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
    after
      :sys.replace_state(source, fn state -> %{state | ctx: original} end)
    end

    {:ok, _} = Streams.append("/v1/stream/source", "later")
    {:ok, {:completed, :created, _}} = call(c.target, {:activate, c.target, cap})
    assert {:ok, %{messages: [{0, "prefix"}, {10, "own"}]}} = Streams.read("/v1/stream/target", 0)
  end

  test "sibling registrations do not enlarge stream metadata or read views" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")

    for n <- 1..100 do
      {:ok, :created, _} = Streams.create("/sibling-#{n}", forked_from: "/source")
    end

    assert byte_size(Codec.encode(meta("/source"))) < 2048
    ctx = ctx_for("/source")
    {:ok, view} = StreamServer.call(ctx, "/source", :read_info, 5_000)
    assert byte_size(:erlang.term_to_binary(view)) < 2048
    assert {:ok, %{messages: [{0, "prefix"}]}} = Streams.read("/sibling-1", 0, max_bytes: 1)
  end

  test "source registration does not receive the target's initial JSON body" do
    {:ok, :created, _} = Streams.create("/source", content_type: "application/json", body: "[1]")
    source = server("/source")
    body = "[\"" <> String.duplicate("x", 128 * 1024) <> "\"]"
    :erlang.trace(source, true, [:receive, {:tracer, self()}])

    try do
      {:ok, :created, _} = Streams.create("/target", forked_from: "/source", body: body)

      assert_receive {:trace, ^source, :receive, {:"$gen_call", _, {:register, command, nil}}}

      assert byte_size(:erlang.term_to_binary(command)) < 2048
      assert {:ok, %{messages: [{0, "1"}, {5, value}]}} = Streams.read("/target", 0)
      assert byte_size(value) == 128 * 1024 + 2
    after
      :erlang.trace(source, false, [:receive])
    end
  end

  test "retained reads reject pending durable source metadata" do
    {:ok, :created, _} = Streams.create("/ancestor", body: "a")
    c = reserve("/source", %{forked_from: "/ancestor", body: "b"})
    pending = meta("/source")
    cap = register(c)
    {:ok, {:completed, :created, _}} = call(c.target, {:activate, c.target, cap})
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    current = meta("/source")
    ctx = ctx_for("/source")
    key = Keys.meta("/source")
    {:ok, _} = SlateDB.put(ctx.db, key, Codec.encode(pending), await_durable: true)

    try do
      assert {:error, :unavailable} = Streams.read("/target", 0)
    after
      {:ok, _} = SlateDB.put(ctx.db, key, Codec.encode(current), await_durable: true)
    end

    assert {:ok, %{messages: [{0, "a"}, {5, "b"}]}} = Streams.read("/target", 0)
  end

  test "a stale sub-offset read resumes a matching caller's durable capture" do
    {:ok, :created, _} = Streams.create("/ancestor", body: "hello")
    {:ok, :created, _} = Streams.create("/source", forked_from: "/ancestor")
    req = %{forked_from: "/source", fork_offset: 0, fork_sub_offset: 3, body: "own"}
    c = reserve("/target", req)
    selection = Target.selection(c)
    {:resolve, view} = call(c.source, {:register, selection, nil})

    assert {:ok, :created, _} =
             during_requests(
               c.source,
               [{{:register, selection, nil}, {:resolve, view}}],
               fn -> Streams.create("/target", Map.to_list(req)) end,
               fn ->
                 {:ok, cap} =
                   call(
                     c.source,
                     {:register, selection, {view.sid, view.trim, view.next_offset, 0, "hel"}}
                   )

                 assert cap.prefix == "hel"
                 :ok = Streams.delete("/source")
               end
             )

    assert {:ok, %{messages: [{0, "hel"}, {7, "own"}]}} = Streams.read("/target", 0)
  end

  test "pages use one budget across a reference boundary and clamp nonboundary cursors" do
    {:ok, :created, _} = Streams.create("/a", body: "hello")
    {:ok, :created, _} = Streams.create("/b", forked_from: "/a", fork_offset: 3, body: "own")

    assert {:ok, %{messages: [{0, "hello"}], next_offset: 3, up_to_date: false}} =
             Streams.read("/b", 0, max_bytes: 5)

    assert {:ok, %{messages: [{3, "own"}], next_offset: 10}} = Streams.read("/b", 3)
    assert {:ok, %{messages: [{0, "hello"}, {3, "own"}]}} = Streams.read("/b", 0, max_bytes: 6)
    assert {:ok, %{messages: [{3, "own"}]}} = Streams.read("/b", 1)
  end

  test "three levels survive trims, deletion, owner reload and expiry of intermediates" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    {:ok, :created, _} = Streams.create("/a", body: "a", ttl_s: 1)
    {:ok, :created, _} = Streams.create("/b", forked_from: "/a", body: "b", ttl_s: 2)
    {:ok, :created, _} = Streams.create("/c", forked_from: "/b", body: "c", ttl_s: 100)
    :ok = Streams.trim("/b", 10)
    :ok = Streams.trim("/a", 5)
    FakeClock.advance(3_000)
    for ctx <- contexts(), do: Expiry.sweep(ctx)
    assert Streams.head("/a") == {:error, :gone}
    assert Streams.head("/b") == {:error, :gone}
    for path <- ["/a", "/b", "/c"], do: kill(server(path))
    for ctx <- contexts(), do: Deleter.drain(ctx)
    assert {:ok, %{messages: [{0, "a"}, {5, "b"}, {10, "c"}]}} = Streams.read("/c", 0)
    assert :ok = Streams.trim("/c", 10)
    assert Streams.read("/c", 0) == {:error, :trimmed}
    assert meta("/a").reclaimed_before == 0
    assert :ok = Streams.delete("/c")
    clean()
  end

  test "inherited JSON and binary sub-offsets use logical reads" do
    {:ok, :created, _} = Streams.create("/j", content_type: "application/json", body: "[1,2,3]")
    {:ok, :created, _} = Streams.create("/jf", forked_from: "/j")

    {:ok, :created, %{next_offset: 10}} =
      Streams.create("/jff", forked_from: "/jf", fork_offset: 0, fork_sub_offset: 2)

    assert {:ok, %{messages: [{0, "1"}, {5, "2"}]}} = Streams.read("/jff", 0)
    {:ok, :created, _} = Streams.create("/b", body: "hello")
    {:ok, :created, _} = Streams.create("/bf", forked_from: "/b")

    {:ok, :created, _} =
      Streams.create("/bff", forked_from: "/bf", fork_offset: 0, fork_sub_offset: 3)

    assert {:ok, %{messages: [{0, "hel"}]}} = Streams.read("/bff", 0)
  end

  test "large inherited JSON sub-offsets use bounded pages" do
    value = fn n -> String.pad_leading(Integer.to_string(n), 24, "0") end
    body = JSON.encode!(Enum.map(1..10_001, value))

    {:ok, :created, _} =
      Streams.create("/source", content_type: "application/json", body: body)

    {:ok, :created, _} = Streams.create("/middle", forked_from: "/source")
    :erlang.trace_pattern({Streams, :read_identity, 6}, true, [:local])

    task =
      Task.async(fn ->
        receive do
          :go ->
            Streams.create("/target",
              forked_from: "/middle",
              fork_offset: 0,
              fork_sub_offset: 10_000
            )
        end
      end)

    :erlang.trace(task.pid, true, [:call, {:tracer, self()}])
    send(task.pid, :go)

    try do
      assert {:ok, :created, %{next_offset: offset}} = Task.await(task, 30_000)
      barrier = :erlang.trace_delivered(task.pid)
      assert_receive {:trace_delivered, _, ^barrier}
      pages = collect_resolution_pages([])
      assert Enum.count(pages) in 2..19
      assert Enum.all?(pages, fn [_, _, _, max_bytes, _, _] -> max_bytes <= 64 * 1024 end)
      [[identity, view, 0, _, first_timeout, cluster] | _] = pages

      assert Enum.all?(pages, fn [source, captured, _, _, timeout, owner] ->
               source == identity and captured == view and timeout <= first_timeout and
                 owner == cluster
             end)

      assert offset == 30 * 10_000
      expected = JSON.encode!(value.(10_000))

      assert {:ok, %{messages: [{_, ^expected}], next_offset: ^offset}} =
               Streams.read("/target", offset - 30)
    after
      :erlang.trace_pattern({Streams, :read_identity, 6}, false, [:local])
    end
  end

  test "JSON sub-offsets stop exactly within pages and at inherited boundaries" do
    {:ok, :created, _} = Streams.create("/source", content_type: "application/json", body: "[1]")

    {:ok, :created, _} =
      Streams.create("/middle", forked_from: "/source", fork_offset: 3, body: "[2,3]")

    for {sub, offset, expected} <- [{1, 3, ["1"]}, {2, 8, ["1", "2"]}, {3, 13, ["1", "2", "3"]}] do
      path = "/target#{sub}"

      assert {:ok, :created, %{next_offset: ^offset}} =
               Streams.create(path, forked_from: "/middle", fork_offset: 0, fork_sub_offset: sub)

      assert {:ok, page} = Streams.read(path, 0)
      assert bodies(page) == expected
    end

    assert {:error, {:bad_request, :invalid_fork_sub_offset}} =
             Streams.create("/past-tail",
               forked_from: "/middle",
               fork_offset: 0,
               fork_sub_offset: 4
             )
  end

  @tag child_options: [max_fork_depth: 2]
  test "depth rejects above the limit without a pin, and manual copies reset it" do
    {:ok, :created, _} = Streams.create("/a", body: "a")
    {:ok, :created, _} = Streams.create("/b", forked_from: "/a")
    {:ok, :created, _} = Streams.create("/c", forked_from: "/b")
    assert Meta.depth(meta("/c")) == 2

    assert {:error, {:bad_request, :max_fork_depth_exceeded}} =
             Streams.create("/d", forked_from: "/c")

    assert registrations("/c") == []
    assert Streams.head("/d") == {:error, :unavailable}
    assert :ok = Streams.delete("/d")
    {:ok, :created, _} = Streams.copy("/c", "/copy")
    assert Meta.depth(meta("/copy")) == 0
    {:ok, :created, _} = Streams.create("/d", forked_from: "/copy")
    assert Meta.depth(meta("/d")) == 1
    {:ok, :created, _} = Streams.create("/empty", forked_from: "/c", fork_offset: 0)
    assert Meta.depth(meta("/empty")) == 0
  end

  test "reservations stay hidden, preserve initial content and resume the captured prefix" do
    {:ok, :created, _} = Streams.create("/group/.source", body: "hello")
    req = %{forked_from: "/group/.source", body: "body"}
    c = reserve("/group/.target", req)
    assert Streams.head(c.target.path) == {:error, :unavailable}
    assert Streams.read(c.target.path, 0) == {:error, :unavailable}
    assert Streams.append(c.target.path, "no") == {:error, :unavailable}
    assert Streams.create("/desc", forked_from: c.target.path) == {:error, :unavailable}
    assert Streams.list("/group/.") == {:ok, ["/group/.source"]}
    cap = register(c)
    kill(server(c.target.path))
    {:ok, _} = Streams.append(c.source.path, "later")
    :ok = Streams.trim(c.source.path, 9)
    :ok = Streams.delete(c.source.path)

    assert {:error, :conflict} =
             Streams.create(c.target.path, forked_from: c.source.path, body: "different")

    assert {:ok, :created, %{next_offset: 17}} = Streams.create(c.target.path, Map.to_list(req))
    assert {:ok, %{messages: [{0, "hello"}, {9, "body"}]}} = Streams.read(c.target.path, 0)
    assert Meta.reference(meta(c.target.path)) == cap
  end

  test "a persisted binary prefix survives a trim beyond the unpinned source message" do
    {:ok, :created, _} = Streams.create("/source", body: "hello")

    c =
      reserve("/target", %{forked_from: "/source", fork_offset: 0, fork_sub_offset: 3, body: "!"})

    {:resolve, view} = call(c.source, {:register, Target.selection(c), nil})

    {:ok, cap} =
      call(
        c.source,
        {:register, Target.selection(c), {view.sid, view.trim, view.next_offset, 0, "hel"}}
      )

    assert cap.prefix == "hel"
    :ok = Streams.trim("/source", 9)
    for ctx <- contexts(), do: Deleter.drain(ctx)
    kill(server("/target"))

    assert {:ok, :created, _} =
             Streams.create("/target",
               forked_from: "/source",
               fork_offset: 0,
               fork_sub_offset: 3,
               body: "!"
             )

    assert {:ok, %{messages: [{0, "hel"}, {7, "!"}]}} = Streams.read("/target", 0)
  end

  test "publication releases temporary payloads while retaining retry and read identity" do
    prefix = :crypto.strong_rand_bytes(128 * 1024)
    body = :crypto.strong_rand_bytes(128 * 1024)
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, %{next_offset: source_tail}} = Streams.append("/source", prefix)

    opts = [
      forked_from: "/source",
      fork_offset: 5,
      fork_sub_offset: byte_size(prefix),
      body: body
    ]

    {:ok, :created, %{next_offset: tail}} = Streams.create("/target", opts)

    assert byte_size(Codec.encode(meta("/target"))) < 4096
    assert {:ok, %{messages: [{0, "a"}, {5, ^prefix}, {_, ^body}]}} = Streams.read("/target", 0)
    repair_until(fn -> Enum.all?(registrations("/source"), &(&1.prefix == nil)) end)
    assert byte_size(Codec.encode(meta("/source"))) < 4096
    kill(server("/target"))
    assert {:ok, :exists, _} = Streams.create("/target", opts)
    assert {:ok, %{messages: [{0, "a"}, {5, ^prefix}, {_, ^body}]}} = Streams.read("/target", 0)

    :ok = Streams.trim("/target", tail)
    :ok = Streams.trim("/source", source_tail)
    for ctx <- contexts(), do: Deleter.drain(ctx)

    assert [] ==
             Read.read_msgs(ctx_for("/target").db, meta("/target").sid, 0, tail, tail) |> elem(0)

    assert [_] = registrations("/source")
    :ok = Streams.delete("/target")
    :ok = Streams.delete("/source")
    clean()
  end

  test "repair checks other registrations while one target is suspended" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    source_ctx = ctx_for("/source")
    repair_job = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, source_ctx.n, :repair))
    :ok = :sys.suspend(repair_job)
    c = reserve("/ghost", %{forked_from: "/source"})
    register(c)
    :ok = Streams.delete("/ghost")
    {:ok, :created, _} = Streams.create("/slow", forked_from: "/source")

    assert [ghost_reg, slow_reg] =
             Enum.sort_by(registrations("/source"), & &1.target.path)

    assert slow_reg.target.path == "/slow"
    assert ghost_reg.target == c.target
    source = server("/source")
    slow = server("/slow")
    :ok = :sys.suspend(slow)
    :erlang.trace_pattern({Retention, :unregister, 4}, true, [:local])
    :erlang.trace(source, true, [:call, {:tracer, self()}])
    :ok = :sys.resume(repair_job)
    sweep = Task.async(fn -> Repair.sweep(source_ctx) end)

    try do
      target = c.target
      assert_receive {:trace, ^source, :call, {Retention, :unregister, [_, _, _, ^target]}}, 5_000
      :sys.get_state(source)
      :ok = SlateDB.flush(source_ctx.db)
      assert [remaining] = registrations("/source")
      assert remaining.target.path == "/slow"
    after
      :erlang.trace(source, false, [:call])
      :erlang.trace_pattern({Retention, :unregister, 4}, false, [:local])
      :sys.resume(slow)
      Task.await(sweep, 10_000)
    end
  end

  test "a seal after reservation prevents publication and releases protection" do
    {:ok, :created, _} = Streams.create("/source", body: "x")
    c = reserve("/g/.target", %{forked_from: "/source"})
    cap = register(c)
    assert :ok = Streams.seal("/g")
    assert {:error, :sealed} = call(c.target, {:activate, c.target, cap})
    repair_until(%{"/source" => 0, c.target.path => nil})
    assert Streams.head(c.target.path) == {:error, :not_found}
    assert registrations("/source") == []
  end

  test "a delayed reference activation cannot publish into a replacement reservation" do
    {:ok, :created, _} = Streams.create("/source", body: "old")
    c = reserve("/target", %{forked_from: "/source", body: "old body"})
    cap = register(c)
    :ok = Streams.delete("/target")
    repair_until(%{"/source" => 0, "/target" => nil})
    replacement = reserve("/target", %{forked_from: "/source", body: "new body"})
    assert replacement.target != c.target
    assert {:error, :conflict} = call(c.target, {:activate, c.target, cap})
    assert Streams.head("/target") == {:error, :unavailable}

    assert {:ok, :created, _} =
             Streams.create("/target", forked_from: "/source", body: "new body")

    assert {:ok, %{messages: [{0, "old"}, {7, "new body"}]}} = Streams.read("/target", 0)
  end

  test "a deleted target during a retained scan cannot return a partial page" do
    {:ok, :created, _} = Streams.create("/source", body: "old")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    ctx = ctx_for("/target")
    {:ok, view} = StreamServer.call(ctx, "/target", :read_info, 5_000)
    :ok = Streams.delete("/target")
    repair_until(%{"/target" => nil})
    start_supervised!({Registry, keys: :unique, name: __MODULE__.Views})
    held = %{ctx | registry: __MODULE__.Views}
    start_supervised!({Slap.Streams.Test.ReadView, {held, "/target", view}})
    assert {:error, :not_found} = Reader.read(held, "/target", 0, 1024, 5_000)
  end

  test "inherited pages do not depend on ancestor stream servers" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")
    {:ok, :created, _} = Streams.create("/middle", forked_from: "/source", body: "middle")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/middle", body: "own")
    :ok = Streams.trim("/source", 10)
    :ok = Streams.delete("/middle")
    ancestors = for path <- ["/source", "/middle"], do: {path, server(path)}
    for {_, pid} <- ancestors, do: :sys.suspend(pid)

    try do
      assert {:ok, %{messages: [{0, "prefix"}, {10, "middle"}, {20, "own"}]}} =
               Streams.read("/target", 0, timeout: 1_000)

      assert {:ok, %{messages: [{20, "own"}]}} = Streams.read("/target", 20)
    after
      for {_, pid} <- ancestors, do: :sys.resume(pid)
    end

    for {_, pid} <- ancestors, do: GenServer.stop(pid, :normal, 5_000)

    assert {:ok, %{messages: [{0, "prefix"}, {10, "middle"}, {20, "own"}]}} =
             Streams.read("/target", 0)

    for {path, _} <- ancestors, do: assert(server(path) == nil)
  end

  test "sub-offset resolution stays within its captured durable source view" do
    {:ok, :created, _} = Streams.create("/source", body: "first")
    ctx = ctx_for("/source")
    {:ok, view} = StreamServer.call(ctx, "/source", :read_info, 5_000)
    {:ok, identity} = Streams.internal("/source", :source_identity)
    {:ok, _} = Streams.append("/source", "later")

    assert {:ok, %{messages: [], next_offset: 9}} =
             Streams.read_identity(identity, view, 9, 1, 5_000, Streams.Cluster)
  end

  test "repair checks preserve expiry access semantics and never touch retained ancestors" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    {:ok, :created, _} = Streams.create("/a", body: "a", ttl_s: 1)
    {:ok, :created, _} = Streams.create("/b", forked_from: "/a", ttl_s: 100)
    FakeClock.advance(2_000)
    repair_until(%{"/a" => 1})
    assert Streams.list("/a") == {:ok, ["/a"]}
    assert Streams.head("/a") == {:error, :gone}
    assert {:ok, %{messages: [{0, "a"}]}} = Streams.read("/b", 0)
  end

  @tag child_options: [creation_grace: 0]
  test "repair durably cancels interrupted reservations, registrations and partial copies" do
    {:ok, :created, _} = Streams.create("/source", body: "x")
    c1 = reserve("/reserved", %{forked_from: "/source"})
    c2 = reserve("/registered", %{forked_from: "/source"})
    cap2 = register(c2)
    c3 = reserve("/partial", %{forked_from: "/source"}, :copy)
    cap = register(c3)
    {:ok, {:pending, _}} = call(c3.target, {:capture, c3.target, cap})
    {:ok, {:pending, _}} = call(c3.target, {:copy_page, c3.target, 0, [{0, "x"}], 5})
    for c <- [c1, c2, c3], do: kill(server(c.target.path))
    repair_until(%{"/source" => 0})
    assert registrations("/source") == []

    for c <- [c1, c2, c3] do
      assert Streams.head(c.target.path) == {:error, :not_found}
    end

    assert {:error, :conflict} = call(c2.target, {:activate, c2.target, cap2})
    assert {:error, :conflict} = call(c3.target, {:activate, c3.target})
    :ok = Streams.delete("/source")
    clean()
  end

  test "a stale absent view cannot release protection through an unwritable former handle" do
    {:ok, :created, _} = Streams.create("/source", body: "retained")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    {:ok, closed_db} = SlateDB.open("former-owner", store: :memory)
    :ok = SlateDB.close(closed_db)
    {:ok, stale_db} = SlateDB.open("stale-view", store: :memory)
    on_exit(fn -> SlateDB.close(stale_db) end)
    pid = server("/target")
    ref = Process.monitor(pid)
    target = Meta.reference(meta("/target")).target

    :sys.replace_state(pid, fn state ->
      absent = Stream.absent("/target")
      %{state | ctx: %{state.ctx | db: closed_db}, stream: absent, durable: Stream.view(absent)}
    end)

    assert {:error, :unavailable} =
             Repair.target_status(%{ctx_for("/target") | db: stale_db}, target)

    Repair.sweep(ctx_for("/source"))
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    assert [_] = registrations("/source")
    :ok = Streams.trim("/source", 12)
    Deleter.drain(ctx_for("/source"))
    assert {:ok, %{messages: [{0, "retained"}]}} = Streams.read("/target", 0)
  end

  test "closed sources can fork, and waits observe only the fork's own tail" do
    {:ok, :created, _} = Streams.create("/source", body: "x", closed: true)
    {:ok, :created, %{closed: false}} = Streams.create("/fork", forked_from: "/source")
    {:ok, {:waiting, pending}} = Streams.wait("/fork", :now)
    ref = Wait.ref(pending)
    {:ok, _} = Streams.append("/fork", "own")
    assert_receive {:slap_streams_wake, ^ref, :data}
    {:ok, :created, _} = Streams.create("/open", body: "a")
    {:ok, :created, _} = Streams.create("/open-fork", forked_from: "/open")
    {:ok, {:waiting, pending}} = Streams.wait("/open-fork", :now)
    ref = Wait.ref(pending)
    {:ok, _} = Streams.append("/open", "later")
    {:ok, _} = Streams.head("/open")
    refute_received {:slap_streams_wake, ^ref, _}
    {:ok, _} = Streams.append("/open-fork", "own")
    assert_receive {:slap_streams_wake, ^ref, :data}
  end

  test "concurrent matching creates capture one prefix and initial body exactly once" do
    {:ok, :created, _} = Streams.create("/source", body: "prefix")
    caller = self()

    tasks =
      for _ <- 1..2 do
        Task.async(fn ->
          send(caller, {:ready, self()})

          receive do
            :go -> Streams.create("/target", forked_from: "/source", body: "initial")
          end
        end)
      end

    for _ <- tasks, do: assert_receive({:ready, _pid})
    for task <- tasks, do: send(task.pid, :go)
    replies = Enum.map(tasks, &Task.await(&1, 10_000))
    assert Enum.any?(replies, &match?({:ok, :created, _}, &1))
    assert Enum.all?(replies, &(match?({:ok, _, _}, &1) or &1 == {:error, :conflict}))
    assert {:ok, %{messages: [{0, "prefix"}, {10, "initial"}]}} = Streams.read("/target", 0)
    assert [_] = registrations("/source")
  end

  test "source and target routing overrides survive source removal and owner reload" do
    source_key = "source-override"

    target_key =
      Enum.find(
        for(i <- 1..100, do: "target-override-#{i}"),
        &(Streams.Cluster.shard_for(&1) != Streams.Cluster.shard_for(source_key))
      )

    source_opts = [placement_key: source_key]
    target_opts = [placement_key: target_key, source_placement_key: source_key]
    {:ok, :created, _} = Streams.create("/source", [body: "routed"] ++ source_opts)
    {:ok, :created, _} = Streams.create("/target", [forked_from: "/source"] ++ target_opts)
    {:ok, :created, _} = Streams.copy("/target", "/copy", source_placement_key: target_key)
    :ok = Streams.delete("/source", source_opts)

    assert {:ok, %{messages: [{0, "routed"}]}} =
             Streams.read("/target", 0, placement_key: target_key)

    assert {:ok, %{messages: [{0, "routed"}]}} = Streams.read("/copy", 0)
  end

  @tag timeout: 180_000
  test "forks and manual copies above 64 MiB, including a copy of a reference" do
    {:ok, :created, _} = Streams.create("/large")
    bytes = :binary.copy("x", 1024 * 1024)
    for _ <- 1..65, do: {:ok, _} = Streams.append("/large", bytes)
    {:ok, :created, %{next_offset: tail}} = Streams.create("/large-fork", forked_from: "/large")
    assert tail > 64 * 1024 * 1024
    assert {:ok, :created, %{next_offset: ^tail}} = Streams.copy("/large-fork", "/large-copy")
    :ok = Streams.delete("/large-fork")
    :ok = Streams.delete("/large")
    repair_until(%{"/large" => nil, "/large-fork" => nil})

    assert {:ok, %{messages: messages, next_offset: ^tail}} =
             Streams.read("/large-copy", 0, max_bytes: 128 * 1024 * 1024)

    assert Enum.count(messages) == 65
    assert Enum.all?(messages, fn {_, b} -> b == bytes end)
  end

  @tag shards: 1
  test "zero bounds remain absent through publication, pinned trims and registration release" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/fork", forked_from: "/source")
    {:ok, :created, _} = Streams.copy("/source", "/copy")
    ctx = ctx_for("/source")
    repair_until(%{"/source" => 1})
    :ok = Streams.trim("/source", 5)
    Deleter.drain(ctx)

    for path <- ["/source", "/fork", "/copy"] do
      {:ok, meta} = Read.get_meta(ctx.db, path, durability: :remote)
      assert meta.reclaimed_before == 0
      assert {:ok, nil} = SlateDB.get(ctx.db, Keys.reclaim(meta.sid), durability: :remote)
    end

    assert {:ok, %{messages: [{0, "a"}]}} = Streams.read("/fork", :start)
    {:ok, :created, _} = Streams.create("/untrimmed", body: "b")
    {:ok, :created, _} = Streams.create("/released", forked_from: "/untrimmed")
    :ok = Streams.delete("/released")
    repair_until(%{"/untrimmed" => 0})
    {:ok, meta} = Read.get_meta(ctx.db, "/untrimmed", durability: :remote)
    assert {:ok, nil} = SlateDB.get(ctx.db, Keys.reclaim(meta.sid), durability: :remote)
    Deleter.drain(ctx)
    :ok = Streams.delete("/fork")
    repair_until(%{"/source" => 0})
    Deleter.drain(ctx)
    assert {:ok, nil} = SlateDB.get(ctx.db, Keys.msg(1, 0, 0))
    assert {:ok, %{messages: [{0, "a"}]}} = Streams.read("/copy", :start)
  end

  defp collect_resolution_pages(pages) do
    receive do
      {:trace, _, :call, {Streams, :read_identity, args}} ->
        collect_resolution_pages([args | pages])
    after
      0 -> Enum.reverse(pages)
    end
  end

  defp clean do
    repair_until(fn -> retention_counts() == %{} end)

    for ctx <- contexts() do
      Deleter.drain(ctx)
      assert [] == ctx.db |> SlateDB.scan(gte: <<1>>) |> Enum.to_list()
    end
  end
end
