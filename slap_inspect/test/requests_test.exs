defmodule Slap.Inspect.RequestsTest do
  use ExUnit.Case, async: true

  alias Slap.Inspect.Requests

  test "replaces waiting requests and ignores old results, including after a stall" do
    client = self()

    worker =
      start_supervised!(
        {Requests,
         client: client,
         read: fn request ->
           send(client, {:started, request.name, self()})

           receive do
             :release -> {:ok, request.name}
           end
         end}
      )

    Requests.submit(worker, 1, %{name: :first, budget: 30_000, opts: []})
    assert_receive {:started, :first, first}
    Requests.submit(worker, 2, %{name: :discarded, budget: 30_000, opts: []})
    Requests.submit(worker, 3, %{name: :latest, budget: 30_000, opts: []})
    send(worker, {:stalled, 1})
    assert_receive {:inspection_stalled, 1}
    send(first, :release)
    assert_receive {:started, :latest, latest}
    send(latest, :release)
    assert_receive {:inspection_result, 3, {:ok, :latest}}
    refute_receive {:inspection_result, 1, _}
    refute_receive {:started, :discarded, _}
  end

  test "quitting discards waiting work while the outstanding read finishes" do
    parent = self()
    client = start_supervised!({Agent, fn -> nil end})

    spec =
      Supervisor.child_spec(
        {Requests,
         client: client,
         read: fn request ->
           send(parent, {:started, request.name, self()})

           receive do
             :release -> {:ok, request.name}
           end
         end},
        restart: :temporary
      )

    coordinator = start_supervised!(spec)
    ref = Process.monitor(coordinator)
    Requests.submit(coordinator, 1, %{name: :outstanding, budget: 30_000, opts: []})
    assert_receive {:started, :outstanding, reader}
    Requests.submit(coordinator, 2, %{name: :waiting, budget: 30_000, opts: []})
    Agent.stop(client)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}
    assert Process.alive?(reader)
    send(reader, :release)
    refute_receive {:started, :waiting, _}
  end

  test "stall diagnostics identify the most recently observed owner and shard" do
    client = self()

    worker =
      start_supervised!(
        {Requests,
         client: client,
         read: fn request ->
           send(client, {:started, request.observer, self()})
           receive do: (:release -> {:ok, :finished})
         end}
      )

    Requests.submit(worker, 1, %{shard: 0, budget: 30_000, opts: []})
    assert_receive {:started, ^worker, reader}
    send(worker, {:inspection_owner, :entry@localhost, 0})
    assert_receive {:inspection_owner, 1, :entry@localhost, 0}
    send(worker, {:inspection_owner, :source@localhost, 1})
    assert_receive {:inspection_owner, 1, :source@localhost, 1}
    send(worker, {:stalled, 1})
    assert_receive {:inspection_stalled, 1, :source@localhost, 1}
    send(reader, :release)
    assert_receive {:inspection_result, 1, {:ok, :finished}}
  end
end
