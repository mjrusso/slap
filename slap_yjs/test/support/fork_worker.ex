defmodule Slap.Yjs.Test.ForkWorker do
  @moduledoc false

  alias Slap.SnapshotLog
  alias Slap.SnapshotLog.{Control, Fork, Store}
  alias Slap.Streams
  alias Slap.Streams.Jobs.{Deleter, Repair}
  alias Slap.Yjs.Store, as: YjsStore

  def pause(source, target, step, test) do
    SnapshotLog.fork_internal(YjsStore.base(source), YjsStore.base(target),
      after_step: fn reached ->
        if reached == step do
          send(test, {:fork_step, self(), step})
          receive do: (:continue -> :ok)
        end
      end
    )
  end

  def cancel(target) do
    store = Store.new(YjsStore.base(target), [])
    deadline = Control.deadline(5_000)

    with {:ok, control} <- Control.read(store, deadline),
         {:ok, _} <- Fork.cancel(store, control, :busy, deadline),
         do: :ok
  end

  def drain do
    for n <- Streams.Cluster.local_shards() do
      {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
      Repair.sweep(ctx)
      Deleter.drain(ctx)
    end

    :ok
  end
end
