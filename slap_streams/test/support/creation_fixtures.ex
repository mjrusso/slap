defmodule Slap.Streams.Test.CreationFixtures do
  @moduledoc false

  import ExUnit.Assertions
  import Slap.Streams.Test.ClusterCase, only: [ctx_for: 1]

  alias Slap.Streams
  alias Slap.Streams.Creation.Target
  alias Slap.Streams.Store.Read

  def reserve(path, req, kind \\ :reference) do
    req = Target.request(req)
    {:ok, source} = Streams.internal(req.forked_from, :source_identity)
    {:ok, {:pending, creation}} = Streams.internal(path, {:reserve, kind, req, source})
    creation
  end

  def register(creation) do
    {:ok, capture} = call(creation.source, {:register, Target.selection(creation), nil})
    capture
  end

  def call(identity, request),
    do: Streams.internal_identity(identity, request, 10_000, Streams.Cluster)

  def registrations(path) do
    Read.registrations(ctx_for(path).db, meta(path).sid, durability: :remote) |> Enum.to_list()
  end

  def meta(path) do
    {:ok, meta} = Read.get_meta(ctx_for(path).db, path, durability: :remote)
    meta
  end

  def during_requests(identity, replies, action, fun) do
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(identity.shard)
    key = {ctx.n, {:stream, identity.path}}
    :ok = GenServer.stop({:via, Registry, {ctx.registry, key}}, :normal, 5_000)
    {:ok, _} = Registry.register(ctx.registry, key, nil)
    delayed = Task.async(action)

    try do
      {preceding, [{request, reply}]} = Enum.split(replies, -1)

      for {request, reply} <- preceding do
        assert_receive {:"$gen_call", from, ^request}, 5_000
        GenServer.reply(from, reply)
      end

      assert_receive {:"$gen_call", from, ^request}, 5_000
      Registry.unregister(ctx.registry, key)
      fun.()
      GenServer.reply(from, reply)
      Task.await(delayed, 5_000)
    after
      Registry.unregister(ctx.registry, key)
    end
  end

  def kill(nil), do: :ok

  def kill(pid) do
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, _, _, _}
  end
end
