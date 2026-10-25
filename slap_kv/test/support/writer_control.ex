defmodule Slap.KV.Test.WriterControl do
  @moduledoc false

  import ExUnit.Assertions
  import Slap.KV.Test.ClusterCase

  alias Slap.KV.Admission
  alias Slap.SlateDB

  def pending(partition, fun), do: pending_writer(partition_writer(partition), fun)

  def pending_writer(writer, fun) do
    :erlang.trace(writer, true, [:receive])
    task = Task.async(fun)
    pid = task.pid
    assert_receive {:trace, ^writer, :receive, {:"$gen_call", {^pid, _}, _}}, 5_000
    :erlang.trace(writer, false, [:receive])
    _ = :sys.get_state(writer)
    task
  end

  def flush(partition) do
    ctx = ctx_for(partition)
    test = self()
    id = make_ref()

    :ok =
      :telemetry.attach(
        id,
        [:slap, :kv, :shard, :load],
        fn _, measurements, _, _ ->
          if measurements.requests == 0, do: send(test, {:retired, id})
        end,
        nil
      )

    try do
      :ok = SlateDB.flush(ctx.db)
      if Admission.snapshot(ctx).requests != 0, do: assert_receive({:retired, ^id}, 5_000)
    after
      :telemetry.detach(id)
    end
  end

  def pause_admission(ctx, step) do
    [{admission, _}] = Registry.lookup(ctx.registry, {ctx.n, :kv_admission})
    test = self()
    id = make_ref()

    :ok =
      :sys.install(
        admission,
        {id,
         fn
           _state, {:in, {:"$gen_call", from, {_owner, request}}}, _name
           when elem(request, 0) == step ->
             send(test, {:paused, id, self(), from})

             receive do
               {:resume, ^id} -> :done
             end

           state, _event, _name ->
             state
         end, nil},
        5_000
      )

    id
  end
end
