defmodule Slap.Streams.ReaderTest do
  # The Streams cluster has a registered name shared by these tests.
  use Slap.Streams.Test.ClusterCase, async: false

  alias Slap.SlateDB
  alias Slap.Streams.Jobs.Deleter
  alias Slap.Streams.Reader
  alias Slap.Streams.Store.{Batch, Keys, Meta, Read}
  alias Slap.Streams.Stream
  alias Slap.Streams.StreamServer
  alias Slap.Streams.Test.ReadView

  @moduletag :capture_log

  setup do
    start_supervised!({Registry, keys: :unique, name: __MODULE__.Views})
    :ok
  end

  defp held_read(path) do
    ctx = ctx_for(path)
    {:ok, view} = StreamServer.call(ctx, path, :read_info, 5_000)
    held = %{ctx | registry: __MODULE__.Views}
    start_supervised!({ReadView, {held, path, view}})
    {ctx, view.sid, fn -> Reader.read(held, path, 0, 1_024, 5_000) end}
  end

  test "a hard deletion returns not_found before and after row cleanup" do
    assert {:ok, :created, _} = Streams.create("/source", body: "old")
    assert {:ok, :created, _} = Streams.create("/fork", forked_from: "/source")
    {ctx, sid, read} = held_read("/source")
    assert :ok = Streams.delete("/fork")
    deleter = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter))
    :ok = :sys.suspend(deleter)

    try do
      assert :ok = Streams.delete("/source")
      assert {:ok, marker} = SlateDB.get(ctx.db, Keys.delete_pending(sid), durability: :remote)
      assert marker != nil
      assert read.() == {:error, :not_found}
    after
      :sys.resume(deleter)
    end

    :ok = Deleter.drain(ctx)
    assert {:ok, nil} = SlateDB.get(ctx.db, Keys.delete_pending(sid))
    assert {:ok, nil} = Read.get_tail(ctx.db, sid)
    assert read.() == {:error, :not_found}
  end

  test "a soft deletion returns gone even when the read view had no forks" do
    assert {:ok, :created, _} = Streams.create("/source", body: "old")
    {ctx, sid, read} = held_read("/source")
    assert {:ok, :created, _} = Streams.create("/fork", forked_from: "/source")
    deleter = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter))
    :ok = :sys.suspend(deleter)

    try do
      assert :ok = Streams.delete("/source")
      assert read.() == {:error, :gone}
    after
      :sys.resume(deleter)
    end

    :ok = Deleter.drain(ctx)
    assert {:ok, nil} = Read.get_tail(ctx.db, sid)
    assert read.() == {:error, :gone}
  end

  test "a replacement's soft deletion does not classify a read of the old stream" do
    assert {:ok, :created, _} = Streams.create("/source", body: "old")
    {ctx, _sid, read} = held_read("/source")
    assert :ok = Streams.delete("/source")
    :ok = Deleter.drain(ctx)
    assert {:ok, :created, _} = Streams.create("/source", body: "new")
    assert read.() == {:error, :not_found}
    assert {:ok, :created, _} = Streams.create("/fork", forked_from: "/source")
    assert :ok = Streams.delete("/source")
    assert read.() == {:error, :not_found}
    assert Streams.read("/source", 0) == {:error, :gone}
  end

  @tag settings: %{flush_interval: "1h"}
  test "classification ignores finalization that is not durable" do
    path = "/source"
    ctx = ctx_for(path)
    meta = %Meta{sid: 1, forks: ["/fork"]}
    view = %Stream.View{status: :active, meta: meta, sid: 1, next_offset: 0, trim: 0}
    held = %{ctx | registry: __MODULE__.Views}
    start_supervised!({ReadView, {held, path, view}})

    assert {:ok, _} = SlateDB.write(ctx.db, Batch.soft_delete(path, meta, nil))
    assert :ok = SlateDB.flush(ctx.db)
    assert {:ok, _} = SlateDB.write(ctx.db, Batch.finalize(path, meta.sid))
    assert {:ok, nil} = Read.get_meta(ctx.db, path)
    assert Reader.read(held, path, 0, 1_024, 5_000) == {:error, :gone}
  end
end
