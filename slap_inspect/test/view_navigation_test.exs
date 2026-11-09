defmodule Slap.Inspect.ViewNavigationTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias Breeze.Test

  test "failed navigation does not show rows from the previous query" do
    session = start_view(size: {140, 36})
    answer(session, {:ok, page()})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "00000000"
    edit(session, "yjs --content --base /missing")
    Test.input(session, "Enter")
    answer(session, {:error, :stream_not_observed})
    screen = Test.render!(session)
    assert screen =~ "stream_not_observed"
    refute screen =~ "00000000"
    refute screen =~ "durable seq 4"
    refute screen =~ ~s(1 raw "a")
  end

  test "the records title shows the position, page and whether more pages follow" do
    session = start_view(size: {140, 36})
    rows = for key <- ["a", "b", "c"], do: %{row() | key: key}
    answer(session, {:ok, %{page() | rows: rows}})
    assert Test.render!(session) =~ "Records · 1/3 · page 1 · n more · Focused"
    Test.input(session, "End")
    assert selected_label(Test.render!(session)) == ~s(3 raw "c")
    assert Test.render!(session) =~ "Records · 3/3 · page 1"
    Test.input(session, "Home")
    assert selected_label(Test.render!(session)) == ~s(1 raw "a")

    Test.input(session, "n")
    answer(session, {:ok, %{page() | rows: rows, cursor: nil}})
    screen = Test.render!(session)
    assert screen =~ "Records · 1/3 · page 2 · Focused"
    Test.input(session, "n")
    assert Test.render!(session) =~ "This is the last page"
    refute_receive {:read, _, _}
  end

  test "disconnected views report the failure and retry until reconnected" do
    session = start_view()
    answer(session, {:error, {:disconnected, :slap@localhost}})
    screen = Test.render!(session)
    assert screen =~ "Nothing read: the read failed."
    assert screen =~ "Reconnecting every 2 seconds"
    refute screen =~ "No rows here"
    assert Test.metadata(session).assigns.refresh_timer

    fire_refresh(session)
    answer(session, {:ok, page()})
    assert Test.render!(session) =~ ~s(1 raw "a")
    refute Test.metadata(session).assigns.refresh_timer
  end

  test "a server with another inspection API version names that version" do
    session = start_view(size: {140, 36})
    answer(session, {:error, {:unsupported_api, 2}})
    assert Test.render!(session) =~ "The server supports inspection API version 2"
  end

  test "refresh preserves selection and navigation uses remembered page boundaries" do
    session = start_view(size: {140, 36})
    answer(session, {:ok, page()})
    Test.input(session, "ArrowDown")
    Test.input(session, "r")
    %{opts: opts} = answer(session, {:ok, page()})
    refute Keyword.has_key?(opts, :cursor)
    assert Test.render!(session) =~ ~r/Key\s+"a"/
    Test.input(session, "n")
    %{opts: opts} = answer(session, {:ok, page()})
    assert opts[:cursor] == "b"
    Test.input(session, "p")
    %{opts: opts} = answer(session, {:ok, page()})
    refute Keyword.has_key?(opts, :cursor)
  end

  test "returning from a link restores the previous view's pagination history" do
    session = start_view()

    linked = fn cursor ->
      {:ok, %{page() | cursor: cursor, rows: [%{row() | decoded: {:ok, %{path: "/linked"}}}]}}
    end

    for {cursor, next} <- [{nil, "b"}, {"b", "c"}, {"c", "d"}] do
      assert %{view: :raw, opts: opts} = answer(session, linked.(next))
      assert opts[:cursor] == cursor
      if next != "d", do: Test.input(session, "n")
    end

    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{view: :logical, opts: opts} = answer(session, linked.("linked-next"))
    assert opts[:path] == "/linked"
    refute Keyword.has_key?(opts, :cursor)
    Test.input(session, "n")
    assert %{view: :logical, opts: opts} = answer(session, linked.(nil))
    assert opts[:cursor] == "linked-next"
    Test.input(session, "b")

    for cursor <- ["c", "b", nil] do
      assert %{view: :raw, opts: opts} = answer(session, linked.(nil))
      assert opts[:cursor] == cursor
      if cursor, do: Test.input(session, "p")
    end
  end

  test "returning from a link restores the original row selection" do
    rows = [row(), %{row() | key: "b", decoded: {:ok, %{path: "/linked"}}}]
    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: rows}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{view: :logical} = answer(session, {:ok, page()})
    Test.input(session, "b")
    answer(session, {:ok, %{page() | rows: rows}})
    assert selected_label(Test.render!(session)) =~ ~r/^2 /
  end

  test "Escape closes the editor and details but does not return to the previous query" do
    session = start_view()
    answer(session, {:ok, %{page() | rows: [%{row() | decoded: {:ok, %{path: "/linked"}}}]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{view: :logical} = answer(session, {:ok, page()})
    Test.input(session, "Enter")
    Test.input(session, "/")
    Test.input(session, "Escape")
    Test.input(session, "Escape")
    Test.input(session, "Escape")
    refute_receive {:read, _, _}
    assert Test.render!(session) =~ "Query: logical"
    Test.input(session, "b")
    assert %{view: :raw} = answer(session, {:ok, page()})
  end

  test "selection keys pressed while a page loads apply to that page in order" do
    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: [%{row() | decoded: {:ok, %{path: "/linked"}}}]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert_receive {:read, %{view: :logical}, worker}
    Test.input(session, "ArrowDown")
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    rows = for key <- ["a", "b", "c"], do: %{row() | key: key}
    reply(session, worker, {:ok, %{page() | rows: rows}})
    screen = Test.render!(session)
    assert selected_label(screen) == ~s(3 raw "c")
    assert screen =~ "Details · bytes · Focused"
  end

  test "switching queries discards selection keys queued during a read" do
    rows = for key <- ["a", "b"], do: %{row() | key: key}

    session = start_view(size: {140, 36})
    assert_receive {:read, _request, worker}
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    edit(session, "streams")
    reply(session, worker, {:ok, %{page() | rows: rows}})
    assert Test.render!(session) =~ "Command: streams_"
    refute_receive {:read, _, _}

    session = start_view(size: {140, 36})
    assert_receive {:read, _request, first}
    Test.input(session, "ArrowDown")
    edit(session, "streams")
    Test.input(session, "Enter")
    send(first, {:reply, {:ok, page()}})
    assert_receive {:read, %{view: :streams}, worker}
    reply(session, worker, {:ok, %{page() | rows: rows}})
    assert selected_label(Test.render!(session)) =~ ~r/^1 /
  end

  test "timed refresh waits for the active call and stalled navigation stays responsive" do
    session = start_view(refresh: 30_000)
    assert_receive {:read, _request, first}
    assert Test.render!(session) =~ "Loading"
    fire_refresh(session)
    # The worker's stall timer sends this after the read's budget.
    Test.info(session, {:inspection_stalled, :active, :owner@localhost, 0})
    assert Test.render!(session) =~ "stalled"
    Test.input(session, "r")
    Test.input(session, "r")
    # Superseded by the waiting refresh, so the worker drops this result.
    send(first, {:reply, {:ok, page()}})
    answer(session, {:ok, page()})
    refute_receive {:read, _, _}
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert Test.render!(session) =~ "This row has no source link"

    for _ <- 1..2 do
      fire_refresh(session)
      answer(session, {:ok, page()})
    end

    Test.input(session, "t")
    fire_refresh(session)
    refute_receive {:read, _, _}
  end

  test "an oversized key prefix remains selectable beside the same ordinary key" do
    ordinary = %{row() | key: "ab"}
    session = start_view(size: {140, 36})

    answer(
      session,
      {:ok, %{page() | rows: [ordinary, %{ordinary | oversized_key: true, value: "prefix"}]}}
    )

    show(session, :term)
    assert Test.render!(session) =~ "oversized_key: false"
    Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ "oversized_key: true"
  end

  test "follow and retention use the displayed shard after custom placement" do
    stream = %{row() | decoded: {:ok, %{path: "/placed", value: %{sid: 7}}}}

    for {key, view} <- [{"f", :logical}, {"g", :retention}] do
      session = start_view(request: [shard: nil, opts: [placement_key: "custom"]])
      answer(session, {:ok, %{page() | shard: 1, rows: [stream]}})
      Test.input(session, "ArrowDown")
      Test.input(session, key)
      assert_receive {:read, %{view: ^view, shard: 1}, _worker}
    end
  end

  test "a files partition row opens that partition's rows" do
    partition = %{
      kind: :files_partition,
      partition: "files-ns:ZGVmYXVsdA:record:demo",
      namespace: "default",
      family: :record,
      suffix: "demo"
    }

    session = start_view(request: [view: :files, opts: [instance: Demo.Files]], size: {140, 36})
    answer(session, {:ok, %{page() | rows: [Map.put(row(), :decoded, {:ok, partition})]}})
    assert Test.render!(session) =~ "1 record default / demo"
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert %{view: :files, opts: opts} = answer(session, {:ok, page()})
    assert Test.render!(session) =~ "b Back"
    assert opts[:partition] == partition.partition
    assert opts[:instance] == Demo.Files
  end

  test "a KV partition row opens that partition's rows" do
    session = start_view(request: [view: :kv], size: {140, 36})

    assert %{opts: []} =
             answer(
               session,
               {:ok,
                %{
                  page()
                  | rows: [
                      Map.put(row(), :decoded, {:ok, %{kind: :partition, partition: "demo"}})
                    ]
                }}
             )

    assert Test.render!(session) =~ "1 demo"
    Test.input(session, "f")
    assert %{view: :kv, opts: opts} = answer(session, {:ok, page()})
    assert opts[:partition] == "demo"
  end

  test "listed logs and Yjs documents open with f" do
    for {view, decoded, label} <- [
          {:log, %{kind: :log, base: "/demo/log"}, "1 /demo/log"},
          {:yjs,
           %{
             kind: :yjs_document,
             base: "/v1/stream/yjs/demo/docs/notes",
             service: "demo",
             name: "notes"
           }, "1 demo / notes"}
        ] do
      session = start_view(request: [view: view], size: {140, 36})
      answer(session, {:ok, %{page() | rows: [Map.put(row(), :decoded, {:ok, decoded})]}})
      assert Test.render!(session) =~ label
      Test.input(session, "ArrowDown")
      Test.input(session, "f")
      assert %{view: ^view, opts: opts} = answer(session, {:ok, page()})
      assert opts[:base] == decoded.base
    end
  end

  test "following an object preserves its Files instance" do
    file = %{row() | decoded: {:ok, %{value: %{body: [:object, "object-key"]}}}}
    session = start_view(request: [view: :files, opts: [instance: NamedFiles]])
    answer(session, {:ok, %{page() | rows: [file]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert_receive {:read, %{view: :object, opts: opts}, _worker}
    assert opts[:instance] == NamedFiles
    assert opts[:object_key] == "object-key"
  end

  test "following a link to one record opens its details" do
    file = %{row() | decoded: {:ok, %{value: %{body: [:object, "object-key"]}}}}
    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: [file]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    object = %{key: "object-key", value: "body", value_size: 4, offset: 0}
    answer(session, {:ok, %{owner: :slap@localhost, object: object}})
    screen = Test.render!(session)
    assert screen =~ "Details · text · Focused"
    assert screen =~ ~s(Key    "object-key")
  end

  test "T reads near the tail and preserves query offsets when paging backward and forward" do
    query = [sid: 1, from: 100, until: 4000]
    session = start_view(request: [view: :messages, opts: query])
    answer(session, {:ok, page()})
    assert Test.render!(session) =~ "T Tail"

    Test.input(session, "T")
    tail = Keyword.put(query, :before, :end)
    assert %{opts: ^tail} = answer(session, {:ok, messages([3000, 3500], before: 4000)})
    assert Test.render!(session) =~ "Records · 2/2 · near tail"

    Test.input(session, "r")
    assert %{opts: ^tail} = answer(session, {:ok, messages([3000, 3500], before: 4000)})

    Test.input(session, "p")
    before = Keyword.put(query, :before, 3000)
    page = messages([100, 2000], before: 3000, cursor: "m3000")
    assert %{opts: ^before} = answer(session, {:ok, page})
    assert Test.render!(session) =~ "page before offset 3000 · n more"

    Test.input(session, "p")
    refute_receive {:read, _, _}
    assert Test.render!(session) =~ "This is the first page"

    Test.input(session, "n")
    assert %{opts: opts} = answer(session, {:ok, messages([3000, 3500], [])})
    assert opts == query ++ [cursor: "m3000", cursor_exclusive: false]
    assert Test.render!(session) =~ "page from offset 3000"

    Test.input(session, "p")
    assert %{opts: ^before} = answer(session, {:ok, page})

    Test.input(session, "T")
    assert %{opts: ^tail} = answer(session, {:ok, messages([3000, 3500], before: 4000)})
    Test.input(session, "T")
    assert %{opts: ^tail} = answer(session, {:ok, messages([3000, 3500], before: 4000)})

    Test.input(session, "b")
    assert %{opts: ^query} = answer(session, {:ok, page()})
  end

  test "switching views preserves custom budgets and replaces defaults" do
    session = start_view(request: [view: :yjs, opts: [base: "/doc"]])
    answer(session, {:ok, page()})
    Test.input(session, "c")
    assert %{view: :yjs_content, budget: 10_000} = answer(session, {:error, :disconnected})
    Test.input(session, "h")
    assert %{view: :home, budget: 10_000} = answer(session, {:error, :disconnected})
    Test.input(session, "1")
    assert %{view: :raw, budget: 5000} = answer(session, {:error, :disconnected})

    session = start_view(request: [view: :raw, budget: 20_000])
    answer(session, {:ok, page()})
    Test.input(session, "h")
    assert %{view: :home, budget: 20_000} = answer(session, {:error, :disconnected})
  end

  defp messages(offsets, fields) do
    rows =
      for offset <- offsets,
          do: %{
            key: "m#{offset}",
            value: "x",
            value_size: 1,
            oversized_key: false,
            decoded: {:ok, %{kind: :message, offset: offset, part: 0, sid: 1}}
          }

    # The server reports a page's first offset as its `from`.
    Map.merge(%{page() | rows: rows, cursor: nil}, Map.new([from: hd(offsets)] ++ fields))
  end
end
