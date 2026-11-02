defmodule Slap.Inspect.ViewClustersTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias Breeze.Test
  alias Slap.Inspect.View.Navigation

  # Menu: 1 raw, 2 streams, 3 log, 4 yjs (Streams); 5 raw, 6 kv, 7 files (KV).
  @clusters [
    %{cluster: Demo.Streams, services: [:streams]},
    %{cluster: Demo.KV, services: [:kv]}
  ]

  defp start(opts \\ []),
    do: start_view(Keyword.merge([clusters: @clusters, size: {140, 36}], opts))

  test "the navigation pane lists each cluster's views and the header names the open cluster" do
    session = start()
    assert %{cluster: Demo.Streams, view: :raw} = answer(session, {:ok, page()})
    screen = Test.render!(session)

    assert screen =~ "Slap Inspect · Demo.Streams · raw"

    for line <-
          ["Demo.Streams", ">1 raw", "2 streams", "3 log", "4 yjs"] ++
            ["Demo.KV", "5 raw", "6 kv", "7 files"],
        do: assert(screen =~ line)
  end

  test "help lists the menu by cluster" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "?")
    screen = Test.render!(session)
    assert screen =~ "Cluster: Demo.Streams"
    assert screen =~ "Demo.Streams: 1 raw · 2 streams · 3 log · 4 yjs"
    assert screen =~ "Demo.KV: 5 raw · 6 kv · 7 files"
  end

  test "a cluster without a service offers only raw reads" do
    session = start(clusters: [%{cluster: Demo.Plain, services: []}])
    answer(session, {:ok, page()})
    screen = Test.render!(session)
    assert screen =~ ">1 raw"
    refute screen =~ "2 streams"
    Test.input(session, "2")
    refute_receive {:read, _, _}
  end

  test "each cluster keeps its own query and page history" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "n")
    assert %{cluster: Demo.Streams, opts: opts} = answer(session, {:ok, page()})
    assert opts[:cursor] == "b"

    Test.input(session, "6")
    assert %{cluster: Demo.KV, view: :kv, opts: opts} = answer(session, {:ok, page()})
    refute Keyword.has_key?(opts, :cursor)
    assert Test.render!(session) =~ "Slap Inspect · Demo.KV · kv"
    Test.input(session, "n")
    assert %{cluster: Demo.KV, opts: [cursor: "b"] ++ _} = answer(session, {:ok, page()})

    Test.input(session, "1")
    assert %{cluster: Demo.Streams, view: :raw, opts: opts} = answer(session, {:ok, page()})
    assert opts[:cursor] == "b"
    Test.input(session, "p")
    assert %{cluster: Demo.Streams, opts: opts} = answer(session, {:ok, page()})
    refute Keyword.has_key?(opts, :cursor)

    Test.input(session, "7")
    assert %{cluster: Demo.KV, view: :files} = answer(session, {:ok, page()})
  end

  test "opening another cluster keeps the selected record of the one it leaves" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Details · bytes · Focused"

    Test.input(session, "5")
    answer(session, {:ok, %{page() | rows: []}})
    refute Test.render!(session) =~ ~s(key: "a")

    Test.input(session, "1")
    answer(session, {:ok, page()})
    screen = Test.render!(session)
    assert screen =~ "Details · bytes · Focused"
    assert selected_label(screen) == ~s(1 raw "a")
  end

  test "another cluster's view that needs arguments opens the editor over that cluster's results" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "5")
    assert %{cluster: Demo.KV, view: :raw} = answer(session, {:ok, %{page() | rows: []}})
    Test.input(session, "4")
    refute_receive {:read, _, _}
    screen = Test.render!(session)
    assert screen =~ "Filter command: yjs --shard 0 --base _"
    assert screen =~ "Slap Inspect · Demo.Streams · raw"
    assert screen =~ ~s(1 raw "a")
  end

  test "a cluster reopens the view it showed, even when a command changed it" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "6")
    assert %{cluster: Demo.KV, view: :kv} = answer(session, {:ok, page()})
    edit(session, "scan")
    Test.input(session, "Enter")
    assert %{cluster: Demo.KV, view: :raw} = answer(session, {:ok, page()})
    assert Test.render!(session) =~ ">5 raw"

    Test.input(session, "1")
    answer(session, {:ok, page()})
    Test.input(session, "6")
    assert %{cluster: Demo.KV, view: :kv} = answer(session, {:ok, page()})
    assert Test.render!(session) =~ ">6 kv"
  end

  test "every assign is either saved per cluster or shared on purpose" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    Test.render!(session)

    # Breeze's own assigns, then the view's shared ones.
    shared =
      ~w(breeze __invalidate__)a ++
        ~w(connection menu saved worker detail_cache mode commands display refresh refresh_timer)a

    saved = Map.keys(Navigation.browsing(nil))
    assigns = Map.keys(Test.metadata(session).assigns)
    assert Enum.sort(assigns -- shared) == Enum.sort(saved)
  end
end
