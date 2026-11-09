defmodule Slap.Cluster do
  @moduledoc false
  def assignments(test), do: ask(test, :assignments)
  def status(test, timeout), do: ask(test, {:status, timeout})

  def ask(test, question) do
    send(test, {question, self()})

    receive do
      {:answer, answer} -> answer
    end
  end
end

defmodule Slap.Streams.Introspect do
  @moduledoc false
  def messages(test, _shard, _version, _budget, opts),
    do: Slap.Cluster.ask(test, {:messages, opts})
end

defmodule Slap.Cluster.Inspection do
  @moduledoc false
  def scan(test, shard, _version, _budget, opts),
    do: Slap.Cluster.ask(test, {:scan, shard, opts})
end

defmodule Slap.Inspect.QueryTest do
  use ExUnit.Case, async: true

  alias Slap.Inspect.{Query, Request}

  test "shard status and assignment reads share the request budget" do
    for {status, expected} <- [
          {{:ok, %{shards: %{0 => %{status: :running}}}}, :running},
          {{:error, :timeout}, :unknown}
        ] do
      task = execute(%{view: :shards, budget: 5000})
      assert_receive {{:status, timeout}, status_caller}
      assert timeout < 5000
      assert_receive {:assignments, caller}
      send(status_caller, {:answer, status})
      send(caller, {:answer, %{0 => {:local, self()}}})
      assert {:ok, [%{shard: 0, status: ^expected}]} = Task.await(task)
    end

    task = execute(%{view: :shards, budget: 1})
    assert_receive {{:status, 1}, status_caller}
    send(status_caller, {:answer, {:error, :timeout}})
    assert_receive {:assignments, caller}
    send(caller, {:answer, %{}})
    assert {:ok, []} = Task.await(task)
  end

  test "the shard picker counts matching rows on each shard" do
    {:ok, listing} =
      Request.new(%{view: :raw, shard: 0, budget: 5000, opts: [cursor: "m", limit: 5]})

    task = execute(Request.shards(listing))
    assert_receive {{:status, _timeout}, status_caller}
    send(status_caller, {:answer, {:error, :timeout}})
    assignments = %{0 => {:local, self()}, 1 => {:local, self()}, 2 => :unassigned}
    assert_receive {:assignments, caller}
    send(caller, {:answer, assignments})

    # Filtered listings can return empty pages with continuation cursors.
    for {shard, cursor, page} <- [
          {0, nil, %{rows: [], cursor: "k"}},
          {0, "k", %{rows: [%{key: "k"}, %{key: "l"}], cursor: nil}},
          {1, nil, %{rows: [%{key: "a"}], cursor: "b"}}
        ] do
      assert_receive {:assignments, caller}
      send(caller, {:answer, assignments})
      assert_receive {{:scan, ^shard, opts}, scanner}
      assert opts[:limit] == 50
      assert opts[:cursor] == cursor
      send(scanner, {:answer, {:ok, page}})
    end

    assert {:ok,
            [
              %{shard: 0, rows: {:exactly, 2}},
              %{shard: 1, rows: {:at_least, 1}},
              %{shard: 2, owner: nil, rows: nil}
            ]} = Task.await(task)
  end

  test "automatic shard selection opens the first nonempty shard without rereading its page" do
    {:ok, listing} = Request.new(%{view: :raw, shard: nil, budget: 5000, opts: []})
    task = execute(%{listing | count_others: true})
    assignments = %{0 => {:local, self()}, 1 => {:local, self()}, 2 => {:local, self()}}
    assert_receive {:assignments, caller}
    send(caller, {:answer, assignments})
    assert_receive {{:scan, 0, _opts}, scanner}
    send(scanner, {:answer, {:ok, %{rows: [], cursor: nil, shard: 0}}})

    for _read <- 1..2 do
      assert_receive {:assignments, caller}
      send(caller, {:answer, assignments})
    end

    assert_receive {{:scan, 1, _opts}, scanner}
    send(scanner, {:answer, {:ok, %{rows: [%{key: "a"}], cursor: nil, shard: 1}}})

    assert {:ok, %{rows: [%{key: "a"}], shard: 1, moved_from: 0}} = Task.await(task)
    refute_received {{:scan, 2, _opts}, _scanner}
  end

  test "empty shard searches preserve the page and counts" do
    {:ok, listing} = Request.new(%{view: :raw, shard: nil, budget: 5000, opts: []})
    task = execute(%{listing | count_others: true})
    assignments = %{0 => {:local, self()}, 1 => {:local, self()}}

    for shard <- [0, nil, 1] do
      assert_receive {:assignments, caller}
      send(caller, {:answer, assignments})

      if shard do
        assert_receive {{:scan, ^shard, _opts}, scanner}
        send(scanner, {:answer, {:ok, %{rows: [], cursor: nil, shard: shard}}})
      end
    end

    assert {:ok, %{rows: [], shard: 0, others: [%{shard: 1, rows: {:exactly, 0}}]} = page} =
             Task.await(task)

    refute Map.has_key?(page, :moved_from)
  end

  test "backward reads fail when the server ignores the before option" do
    {:ok, request} =
      Request.new(%{view: :messages, shard: 0, budget: 5000, opts: [sid: 1, before: :end]})

    read = fn page ->
      task = execute(request)
      assert_receive {:assignments, caller}
      send(caller, {:answer, %{0 => {:local, self()}}})
      assert_receive {{:messages, opts}, reader}
      assert opts[:before] == :end
      send(reader, {:answer, {:ok, page}})
      Task.await(task)
    end

    assert {:ok, %{before: 10}} = read.(%{rows: [], cursor: nil, before: 10})
    assert {:error, :before_unsupported} = read.(%{rows: [], cursor: nil})
  end

  defp execute(request) do
    test = self()
    Task.async(fn -> Query.execute(%{entry: node(), cluster: test}, request) end)
  end
end
