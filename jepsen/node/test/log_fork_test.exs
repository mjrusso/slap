defmodule JepsenNode.LogForkTest do
  use ExUnit.Case, async: false

  alias JepsenNode.{Log, LogFork}
  alias Slap.SnapshotLog.{Control, Store}

  setup do
    start_supervised!(
      {Slap.Streams.Cluster, store: :memory, shards: 2, settings: %{flush_interval: "2ms"}}
    )

    start_supervised!(Slap.Yjs.Docs)
    for child <- Log.child_specs(), do: start_supervised!(child)
    :ok
  end

  for mode <- [:log, :yjs] do
    test "#{mode} forks reconstruct after ancestor deletion and audit cleanup" do
      reconstruct(unquote(mode))
    end
  end

  defp reconstruct(mode) do
    {:ok, %{keys: [a, b, c], shards: [sa, sb, sc]}} =
      LogFork.operation(mode, %{"action" => "plan", "id" => "unit"})

    assert sa != sb and sb != sc
    call(mode, "initialize", a)
    a1 = call(mode, "append", a, %{"value" => 1, "id" => "a1", "client" => 1})

    a2 =
      call(
        mode,
        "append",
        a,
        Map.merge(context(a1), %{"value" => 2, "id" => "a2", "client" => 1})
      )

    a3 =
      if mode == :yjs,
        do:
          call(
            mode,
            "remove",
            a,
            Map.merge(context(a2), %{"value" => 1, "id" => "delete1", "client" => 1})
          ),
        else: a2

    snapshot =
      call(
        mode,
        "compact",
        a,
        Map.merge(context(a3), if(mode == :yjs, do: %{"extra" => 9, "client" => 1}, else: %{}))
      )

    capture = call(mode, "fork", b, %{"source" => a})

    call(
      mode,
      "append",
      b,
      Map.merge(context(snapshot), %{"value" => 3, "id" => "b3", "client" => 2})
    )

    call(mode, "fork", c, %{"source" => b})
    call(mode, "delete", a)
    assert call(mode, "fork", b, %{"source" => a}) == capture
    call(mode, "delete", b)
    expected = if mode == :yjs, do: [2, 9, 3], else: [1, 2, 3]
    assert %{values: ^expected} = call(mode, "read", c)
    if mode == :yjs, do: assert(%{values: ^expected} = call(mode, "join", c))

    assert {:ok, %{rows: 0}} =
             LogFork.operation(mode, %{"action" => "audit", "keys" => [a, b, c]})
  end

  for mode <- [:log, :yjs] do
    test "#{mode} captures a legal cutoff while an append overlaps creation" do
      concurrent_capture(unquote(mode))
    end
  end

  defp concurrent_capture(mode) do
    {:ok, %{keys: [source, target, _]}} =
      LogFork.operation(mode, %{"action" => "plan", "id" => "overlap"})

    call(mode, "initialize", source)
    initial = call(mode, "append", source, %{"value" => 1, "id" => "initial", "client" => 1})

    append =
      Map.merge(context(initial), %{
        "action" => "append",
        "key" => source,
        "value" => 2,
        "id" => "concurrent",
        "client" => 1
      })

    capture = call(mode, "fork", target, %{"source" => source, "concurrent" => append})
    assert capture.offset in [initial.offset, capture.concurrent.offset]
    expected = if capture.offset == initial.offset, do: [1], else: [1, 2]
    assert %{values: ^expected} = call(mode, "read", target)
    assert %{values: [1, 2]} = call(mode, "read", source)
    assert Map.delete(capture, :concurrent) == call(mode, "fork", target, %{"source" => source})
  end

  test "the audit cannot remove leaked fork components" do
    {:ok, %{keys: [source, target, _]}} =
      LogFork.operation(:log, %{"action" => "plan", "id" => "audit"})

    call(:log, "initialize", source)
    call(:log, "append", source, %{"value" => 1, "id" => "one"})
    call(:log, "fork", target, %{"source" => source})
    store = Store.new(Log.base(target), [])
    deadline = Control.deadline(5_000)
    {:ok, control} = Control.read(store, deadline)

    terminal =
      Map.merge(control.record, %{
        "status" => "cancelled",
        "deleted" => true,
        "error" => "deleted",
        "cleanup" => false
      })

    {:ok, _} = Control.put(store, control, terminal, deadline, false)

    assert {:error, :timeout} =
             LogFork.operation(:log, %{
               "action" => "audit",
               "keys" => [source, target],
               "budget_ms" => 1_000
             })

    assert {:ok, %{messages: [{_, "1"}]}} = Slap.Streams.read(Store.path(store, :updates), :start)
  end

  defp context(reply), do: %{"state" => reply[:state], "at" => reply.offset}

  defp call(mode, action, key, rest \\ %{}) do
    {:ok, reply} = LogFork.operation(mode, Map.merge(%{"action" => action, "key" => key}, rest))
    reply
  end
end
