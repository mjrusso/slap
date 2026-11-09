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

    assert screen =~ "Demo.Streams · raw"

    for line <-
          ["│Streams", ">1 raw", "2 streams", "3 log", "4 yjs"] ++
            ["│KV ", "5 raw", "6 kv", "7 files"],
        do: assert(screen =~ line)

    refute screen =~ "[?] Help"
    refute screen =~ "Demo.KV"
    footer = screen |> lines() |> Enum.take(-2) |> Enum.join("\n")
    assert [_, _] = String.split(footer, "? Help")
  end

  test "home shows row counts and opens views with f" do
    session = start(request: [view: :home, shard: nil])

    counts = [
      %{shard: 0, owner: :a@host, rows: {:exactly, 3}},
      %{shard: 1, owner: :a@host, rows: {:exactly, 0}},
      %{shard: 2, owner: nil, rows: nil}
    ]

    rows = [
      home_row(Demo.Streams, :raw, nil),
      home_row(Demo.Streams, :streams, counts),
      home_row(Demo.KV, :kv, [
        %{shard: 0, owner: :a@host, rows: {:exactly, 0}},
        %{shard: 1, owner: :a@host, rows: {:at_least, 50}}
      ])
    ]

    assert %{view: :home} = answer(session, {:ok, %{rows: rows, cursor: nil}})
    screen = Test.render!(session)
    assert screen =~ "home · every cluster"
    assert screen =~ ">h home"
    assert screen =~ "1 Demo.Streams · raw"
    assert screen =~ "2 Demo.Streams · streams · 3 rows on 1 of 3 shards · 1 shar"
    assert screen =~ "3 Demo.KV · kv · 50+ rows on 1 of 2 shards"

    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    screen = Test.render!(session)
    assert screen =~ ~r"Shard 0 +3 rows"
    assert screen =~ ~r"Shard 1 +empty"
    assert screen =~ ~r"Shard 2 +unassigned"

    Test.input(session, "b")
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{cluster: Demo.KV, view: :kv, shard: 1} = answer(session, {:ok, page()})
    assert Test.render!(session) =~ "Demo.KV · kv"

    Test.input(session, "h")
    assert %{cluster: Demo.KV, view: :home} = answer(session, {:ok, %{rows: rows, cursor: nil}})
  end

  test "refreshing home discovers newly started clusters" do
    session = start(clusters: [hd(@clusters)], request: [view: :home, shard: nil])
    clusters = [hd(@clusters), %{cluster: Demo.Later, services: [:kv]}]
    rows = [home_row(Demo.Streams, :raw, nil), home_row(Demo.Later, :kv, nil)]
    answer(session, {:ok, %{rows: rows, cursor: nil, clusters: clusters}})
    assert Test.render!(session) =~ "Later"

    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{cluster: Demo.Later, view: :kv} = answer(session, {:ok, page()})
    edit(session, "kv --partition p")
    Test.input(session, "Enter")

    assert %{cluster: Demo.Later, view: :kv, opts: [partition: "p"]} =
             answer(session, {:ok, page()})
  end

  test "commands report when the selected cluster has stopped" do
    session = start(request: [view: :home, shard: nil])
    rows = [home_row(Demo.KV, :raw, nil)]
    answer(session, {:ok, %{rows: rows, cursor: nil, clusters: tl(@clusters)}})
    refute Test.render!(session) =~ "│Streams"
    edit(session, "scan")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Demo.Streams is no longer running."
    refute_receive {:read, _, _}

    Test.input(session, "Escape")
    Test.input(session, "1")
    assert %{cluster: Demo.KV, view: :raw} = answer(session, {:ok, page()})
  end

  test "switching between shard listings preserves the selected shard" do
    session = start(request: [view: :streams, shard: 1])
    answer(session, {:ok, %{page() | shard: 1}})
    Test.input(session, "3")
    assert %{view: :log, shard: 1} = answer(session, {:ok, %{page() | shard: 1}})
    Test.input(session, "h")
    assert %{view: :home, shard: nil} = answer(session, {:ok, %{rows: [], cursor: nil}})

    session = start(request: [view: :logical, shard: 1, opts: [path: "/a"]])
    answer(session, {:ok, %{page() | shard: 1}})
    Test.input(session, "2")
    assert %{view: :streams, shard: nil} = answer(session, {:ok, page()})
  end

  defp home_row(cluster, view, counts),
    do: %{
      key: "#{inspect(cluster)} #{view}",
      value: <<>>,
      decoded:
        {:ok, %{kind: :cluster_view, cluster: cluster, view: view, shards: 3, counts: counts}}
    }

  test "the header shows the current shard and total shard count" do
    clusters = [%{cluster: Demo.Streams, services: [:streams], shards: 4}]
    session = start(clusters: clusters, request: [shard: nil])
    answer(session, {:error, :disconnected})
    assert Test.render!(session) =~ "Demo.Streams · raw · 4 shards"
    Test.input(session, "r")
    answer(session, {:ok, %{page() | shard: 2}})
    screen = Test.render!(session)
    assert screen =~ "Demo.Streams · raw · shard 2 of 4"
    refute screen =~ "(one shard)"
  end

  test "help lists view shortcuts when the navigation pane is hidden" do
    session = start(size: {80, 24})
    answer(session, {:ok, page()})
    assert Test.render!(session) =~ "1-9 Views"

    session = start(size: {120, 24})
    answer(session, {:ok, page()})
    screen = Test.render!(session)
    refute screen =~ "1-9 Views"
    assert screen =~ "Details · "
  end

  test "unavailable-service errors identify clusters that provide the service" do
    for {clusters, message} <- [
          {@clusters, "Demo.Streams does not run KV; KV runs in Demo.KV."},
          {[hd(@clusters)], "Demo.Streams does not run KV, and no cluster here runs KV."}
        ] do
      session = start(clusters: clusters)
      answer(session, {:ok, page()})
      edit(session, "kv --partition demo")
      Test.input(session, "Enter")
      assert Test.render!(session) =~ message
      refute_receive {:read, _, _}
    end
  end

  test "help lists the menu by cluster" do
    session = start()
    answer(session, {:ok, page()})
    Test.input(session, "?")
    assert Test.render!(session) =~ "Cluster: Demo.Streams"
    Test.input(session, "End")
    screen = Test.render!(session)
    assert screen =~ ~r" {3,}Demo.Streams: 1 raw · 2 streams · 3 log · 4 yjs"
    assert screen =~ ~r"  h +Home: every cluster's views"
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
    assert Test.render!(session) =~ "Demo.KV · kv"
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
        ~w(connection menu saved worker detail_cache mode commands display refresh refresh_interval refresh_timer activity)a

    saved = Map.keys(Navigation.browsing(nil))
    assigns = Map.keys(Test.metadata(session).assigns)
    assert Enum.sort(assigns -- shared) == Enum.sort(saved)
  end
end
