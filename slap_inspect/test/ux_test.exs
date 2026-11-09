defmodule Slap.Inspect.UXTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase,
    only: [start_view: 1, answer: 2, reply: 3, fire_refresh: 1, show: 2, type: 2]

  alias Breeze.Test

  test "a stalled read shows a spinner and its elapsed time in warning colours" do
    session = start_view([])
    # The spinner appears after 200 ms; the stall makes it appear at once.
    assert status_line(session) =~ ~r/^Loading +$/
    Test.info(session, {:inspection_stalled, :active, :owner@localhost, 0})

    assert status_line(session) =~
             ~r/^\e\[38;5;3m. \d+\.\ds · Owner owner@localhost read stalled/u

    answer(session, {:ok, page()})
    assert status_line(session) =~ ~r/^Ready · durable seq 4 +$/
  end

  defp status_line(session), do: session |> Test.render!() |> String.split("\n") |> Enum.at(2)

  test "reenabling refresh restores the configured interval" do
    session = start_view(refresh: 1000)
    Test.input(session, "t")
    assert Test.render!(session) =~ "Refresh off"
    Test.input(session, "t")
    assert Test.render!(session) =~ "Refresh 1000ms"
  end

  test "query, refresh, stale data and diagnostics remain visible at 80 columns" do
    session = start(opts: [partition: "users", key_prefix: <<0>>], view: :kv)
    screen = Test.render!(session)
    assert screen =~ "--partition users --key-prefix 00"
    assert screen =~ "Refresh off"
    assert screen =~ "Last read"
    Test.input(session, "t")
    assert Test.render!(session) =~ "Refresh 5000ms"
    Test.input(session, "r")
    answer(session, {:error, %{reason: :unassigned, status: %{deep: String.duplicate("x", 200)}}})
    assert Test.render!(session) =~ "Last good data"
    assert Test.render!(session) =~ "Unassigned shard"
    refute Test.render!(session) =~ String.duplicate("x", 50)
    Test.input(session, "e")
    assert Test.render!(session) =~ "Diagnostics"
    assert Test.render!(session) =~ "unassigned"
    Test.input(session, "Escape")
    assert Test.render!(session) =~ "Last good data"
  end

  test "Enter opens the first row and a missing selection returns to the list" do
    session = start()
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Details · text · Focused"
    Test.input(session, "r")
    answer(session, {:ok, %{page() | rows: []}})
    screen = Test.render!(session)
    assert screen =~ "Selected record is no longer present"
    assert screen =~ "No rows"
    Test.input(session, "Enter")
    assert Test.render!(session) =~ ~r"Records · .* · Focused"
  end

  test "commands can be edited in place, recalled and cancelled without a read" do
    session = start(view: :kv, opts: [partition: "users"])
    Test.input(session, "/")
    assert Test.render!(session) =~ "kv --shard 0 --partition users_"
    for key <- ["ArrowLeft", "Backspace", "X", "Delete"], do: Test.input(session, key)
    assert Test.render!(session) =~ "kv --shard 0 --partition useX_"
    Test.input(session, "Enter")
    assert_receive {:read, %{opts: opts}, worker}
    assert opts[:partition] == "useX"
    reply(session, worker, {:ok, page()})
    Test.input(session, "/")
    Test.input(session, "\x15")
    type(session, "streams")
    Test.input(session, "Enter")
    assert_receive {:read, %{view: :streams}, worker}
    reply(session, worker, {:ok, page()})
    Test.input(session, "/")
    assert Test.render!(session) =~ "streams --shard 0_"
    Test.input(session, "ArrowUp")
    assert Test.render!(session) =~ "Command: streams_"
    Test.input(session, "ArrowUp")
    assert Test.render!(session) =~ "kv --shard 0 --partition useX_"
    Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ "streams_"
    Test.input(session, "Home")
    assert Test.render!(session) =~ "_streams"
    Test.input(session, "End")
    assert Test.render!(session) =~ "streams_"
    Test.input(session, "Escape")
    refute_receive {:read, _, _}
    assert Test.render!(session) =~ "streams"
  end

  test "following and resubmitting a binary stream path preserves its bytes" do
    session = start(view: :streams)
    stream = Map.put(row(), :decoded, {:ok, %{path: <<255>>, value: %{sid: 7}}})
    Test.input(session, "r")
    answer(session, {:ok, %{page() | rows: [stream]}})
    Test.input(session, "Enter")
    Test.input(session, "f")
    assert_receive {:read, %{view: :logical, opts: opts}, worker}
    assert opts[:path] == <<255>>
    reply(session, worker, {:ok, page()})
    assert Test.render!(session) =~ ~S(--path "\xFF")
    Test.input(session, "/")
    assert Test.render!(session) =~ ~S(--path "\xFF")
    Test.input(session, "Enter")
    assert_receive {:read, %{view: :logical, opts: opts}, worker}
    assert opts[:path] == <<255>>
    reply(session, worker, {:ok, page()})
  end

  test "invalid numeric ranges stay editable and do not start reads" do
    session = start()
    Test.input(session, "/")
    Test.input(session, "\x15")
    type(session, "get --key 61 --offset -1")
    Test.input(session, "Enter")
    screen = Test.render!(session)
    assert screen =~ "--offset must be in 0.."
    assert screen =~ "Enter Submit"
    refute screen =~ "f Follow"
    refute_receive {:read, _, _}
  end

  test "help descriptions use hanging indents at 80 columns" do
    session = start_view(size: {80, 40})
    answer(session, {:ok, page()})
    Test.input(session, "?")
    screen = Test.render!(session)
    assert screen =~ ~r/  T {14}Read near the tail/
    assert screen =~ ~r/ {17}Large messages may need n to reach the end/
  end

  test "help preserves query, selection and scroll position" do
    session = start(value: String.duplicate("0123456789", 100))
    Test.input(session, "Enter")
    Test.input(session, "PageDown")
    before = Test.render!(session)
    Test.input(session, "?")
    assert Test.render!(session) =~ "Keyboard help"
    assert Test.render!(session) =~ "Query: scan"
    Test.input(session, "Escape")
    assert Test.render!(session) == before
  end

  test "text and JSON modes wrap multiline values and retain exact bytes" do
    session = start(value: ~s({"message":"hello\\nworld"}))
    Test.input(session, "Enter")
    show(session, :text)
    assert Test.render!(session) =~ "hello"
    show(session, :json)
    assert Test.render!(session) =~ ~s("message":)
    show(session, :bytes)
    assert Test.render!(session) =~ "00000000"
  end

  test "JSON fallback preserves the chosen view across records" do
    json = %{row() | key: "j", value: ~s({"n":1}), value_size: 7}
    text = %{row() | key: "t", value: "plain words", value_size: 11}
    binary = %{row() | value: <<0, 255, 27>>}
    session = start_view(size: {80, 30})
    answer(session, {:ok, %{page() | rows: [json, text, binary]}})
    Test.input(session, "Enter")
    show(session, :json)

    for {mode, shown} <- [text: "plain words", bytes: "00000000  00 FF 1B"] do
      Test.input(session, "Escape")
      Test.input(session, "ArrowDown")
      Test.input(session, "Enter")
      screen = Test.render!(session)
      assert screen =~ "Details · #{mode}"
      assert screen =~ "Not a complete JSON value; showing #{mode}."
      assert screen =~ shown
    end

    Test.input(session, "Escape")
    Test.input(session, "Home")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Details · json"
  end

  test "d cycles through the selected record's available display modes" do
    session = start(value: <<0, 255>>)
    Test.input(session, "Enter")
    Test.input(session, "d")
    screen = Test.render!(session)
    assert screen =~ "Details · term"
    refute screen =~ "showing"
  end

  test "byte ranges show bounds, reset scrolling and allow returning to the previous range" do
    first = String.duplicate("abcd", 1024)
    session = start(value: first, total: 8192)
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Bytes 0–4095 of 8192"
    Test.input(session, "End")
    Test.input(session, "m")
    assert_receive {:read, %{opts: opts}, worker}
    assert opts[:value_range] == {4096, 4096}
    last = %{row() | value: String.duplicate("defg", 1024), offset: 4096, value_size: 8192}
    reply(session, worker, {:ok, %{row: last}})
    assert Test.render!(session) =~ "Bytes 4096–8191 of 8192"
    Test.input(session, "End")
    Test.input(session, "M")
    assert_receive {:read, %{opts: previous}, worker}
    assert previous[:value_range] == {0, 4096}
    reply(session, worker, {:ok, %{row: %{row() | value: first, value_size: 8192}}})
    assert Test.render!(session) =~ "Bytes 0–4095 of 8192"
  end

  test "byte navigation and refresh preserve the configured value limit" do
    for view <- [:raw, :object] do
      opts = [max_value_bytes: 64, max_key_bytes: 128, target_bytes: 512]
      opts = if view == :object, do: opts ++ [object_key: "a", instance: "files"], else: opts
      session = start(view: view, opts: opts, value: String.duplicate("x", 64), total: 256)
      Test.input(session, "Enter")
      Test.input(session, "m")
      assert_receive {:read, %{view: next_view, opts: next}, worker}
      assert next_view == if(view == :object, do: :object, else: :get)
      assert next[:max_value_bytes] == 64
      assert Keyword.take(next, [:max_key_bytes, :target_bytes]) == []
      assert next[:value_range] == {64, 64}
      if view == :object, do: assert(next[:instance] == "files")

      preview = %{row() | value: String.duplicate("y", 64), offset: 64, value_size: 256}
      field = if view == :object, do: :object, else: :row
      reply(session, worker, {:ok, %{field => preview}})
      Test.input(session, "r")
      assert_receive {:read, %{opts: refresh}, worker}
      assert refresh[:max_value_bytes] == 64
      assert refresh[:value_range] == {64, 64}
      reply(session, worker, {:ok, %{field => preview}})
      Test.input(session, "M")
      assert_receive {:read, %{opts: previous}, worker}
      assert previous[:max_value_bytes] == 64
      assert previous[:value_range] == {0, 64}
      reply(session, worker, {:ok, %{field => %{preview | offset: 0}}})
    end
  end

  test "a missing byte-range read removes the selected record" do
    for {view, field} <- [raw: :row, object: :object] do
      session = start(view: view, total: 6)
      Test.input(session, "Enter")
      Test.input(session, "m")
      answer(session, {:ok, %{field => nil}})
      screen = Test.render!(session)
      assert screen =~ "Selected record is no longer present"
      assert screen =~ "No rows"
      refute screen =~ "Bytes 0–"
      assert screen =~ ~r"Records · .* · Focused"
    end
  end

  test "timed refresh retains the displayed byte range" do
    session = start(value: "abc", total: 9)
    Test.input(session, "Enter")
    Test.input(session, "m")
    assert_receive {:read, _, worker}
    preview = %{row() | value: "def", offset: 3, value_size: 9}
    reply(session, worker, {:ok, %{row: preview}})
    Test.input(session, "t")
    fire_refresh(session)
    assert_receive {:read, %{view: :get, opts: opts}, worker}
    assert opts[:value_range] == {3, 3}
    reply(session, worker, {:ok, %{row: preview}})
    assert Test.render!(session) =~ "Bytes 3–5 of 9"
  end

  test "long header titles leave room for the last read status" do
    cluster = %{cluster: Module.concat([String.duplicate("Long", 30)]), services: [:streams]}
    session = start_view(clusters: [cluster])
    answer(session, {:ok, page()})
    [title, query, status, border | _] = session |> Test.render!() |> String.split("\n")
    assert title =~ ~r/LongLong.* · Refresh off · Last read \d\d:\d\d:\d\d UTC/
    assert query =~ "Query: scan"
    assert status =~ "Ready"
    assert border =~ "┌"
  end

  test "long query arguments use the available header width" do
    session = start(view: :object, opts: [object_key: "objects/" <> String.duplicate("x", 160)])
    assert session |> Test.render!() |> String.split("\n") |> Enum.at(1) =~ "objects/"
  end

  test "long record labels keep the selected row visible" do
    session = start()
    rows = for index <- 1..30, do: %{row() | key: "row#{index}-" <> String.duplicate("x", 160)}
    Test.input(session, "r")
    answer(session, {:ok, %{page() | rows: rows}})
    for _ <- 1..14, do: Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ ~s(15 "row15-)
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "row15-"
  end

  test "view shortcuts discard unrelated filters and restart pagination" do
    session =
      start(view: :kv, opts: [partition: "users", key_prefix: <<0>>, cursor: "old", limit: 1])

    Test.input(session, "2")
    assert_receive {:read, %{view: :streams, opts: opts}, worker}
    assert opts[:limit] == 1
    refute Keyword.has_key?(opts, :partition)
    refute Keyword.has_key?(opts, :key_prefix)
    refute Keyword.has_key?(opts, :cursor)
    reply(session, worker, {:ok, page()})
    assert Test.render!(session) =~ "Query: streams"
  end

  test "opening Yjs from a log catalog clears the selected shard and base" do
    session = start_view(request: [view: :log, shard: 1, opts: [base: "/demo/log"]])
    answer(session, {:ok, page()})
    Test.input(session, "6")
    assert %{view: :yjs, shard: nil, opts: opts} = answer(session, {:ok, page()})
    refute opts[:base]
    assert Test.render!(session) =~ "Query: yjs"
  end

  test "Tab changes the focused pane and its keyboard help" do
    session = start()
    Test.render!(session)
    Test.input(session, "\t")
    terminal = %{session.terminal | size: %{width: 60, height: 18}}
    screen = Test.render!(session, terminal: terminal)
    assert screen =~ "Focused"
    assert screen =~ "Esc List"
    Test.input(session, "\t")
    assert Test.render!(session) =~ ~r"Records · .* · Focused"
  end

  test "shard selection shows available shards and clears the old page cursor" do
    session = start(opts: [cursor: <<0>>, cursor_exclusive: true])
    Test.input(session, "s")

    shards = [
      %{shard: 0, owner: :a@host, status: :running},
      %{shard: 1, owner: :b@host, status: :starting},
      %{shard: 2, owner: nil, status: nil},
      %{shard: 3, owner: :c@host, status: :unknown}
    ]

    answer(session, {:ok, shards})
    screen = Test.render!(session)
    assert screen =~ "Select shard"
    assert screen =~ ">  0  a@host      running"
    assert screen =~ "   2  unassigned"
    assert screen =~ "   3  c@host      status unknown"
    Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ ">  1  b@host      starting"
    Test.input(session, "Enter")
    assert_receive {:read, %{shard: 1, opts: opts}, worker}
    refute opts[:cursor]
    refute opts[:cursor_exclusive]
    reply(session, worker, {:ok, %{page() | shard: 1}})
    assert Test.render!(session) =~ "shard 1"
  end

  test "the shard picker shows matching row counts" do
    session = start(opts: [prefix: "a"])
    Test.input(session, "s")
    assert_receive {:read, %{view: :shards, listing: listing}, worker}
    assert %{operation: :raw, opts: [prefix: "a"]} = listing

    shards = [
      %{shard: 0, owner: :a@host, status: :running, rows: {:exactly, 0}},
      %{shard: 1, owner: :a@host, status: :running, rows: {:exactly, 3}},
      %{shard: 2, owner: :a@host, status: :running, rows: {:at_least, 50}},
      %{shard: 3, owner: :a@host, status: :running, rows: :unchecked}
    ]

    reply(session, worker, {:ok, shards})
    screen = Test.render!(session)
    assert screen =~ ">  0  a@host  running  empty"
    assert screen =~ "   1  a@host  running  3 rows"
    assert screen =~ "   2  a@host  running  50+ rows"
    assert screen =~ "   3  a@host  running  not counted in time"
  end

  test "the shard picker omits counts for single-record views" do
    session = start(view: :stream, opts: [path: "/a"])
    Test.input(session, "s")
    assert_receive {:read, %{view: :shards} = request, _worker}
    refute Map.has_key?(request, :listing)
  end

  test "refresh preserves the initial empty-shard results" do
    session = start_view(request: [view: :streams, shard: nil], refresh: 1000)
    assert_receive {:read, %{count_others: true}, worker}

    others = [
      %{shard: 1, owner: :a@host, rows: {:exactly, 0}},
      %{shard: 2, owner: :a@host, rows: :unchecked},
      %{shard: 3, owner: nil, rows: nil}
    ]

    reply(session, worker, {:ok, %{page() | rows: [], cursor: nil} |> Map.put(:others, others)})
    message = "No rows on shard 0. s counts each shard's rows."
    assert Test.render!(session) =~ message

    fire_refresh(session)
    assert_receive {:read, %{count_others: false}, worker}
    reply(session, worker, {:ok, %{page() | rows: [], cursor: nil}})
    assert Test.render!(session) =~ message

    Test.input(session, "s")

    answer(
      session,
      {:ok,
       [
         %{shard: 0, owner: :a@host, status: :running},
         %{shard: 1, owner: :a@host, status: :running}
       ]}
    )

    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    answer(session, {:ok, %{page() | rows: [], cursor: nil, shard: 1}})
    assert Test.render!(session) =~ "No rows here. s selects another shard."
  end

  test "log listings hide record actions when empty and omit the content action" do
    session = start_view(request: [view: :log], size: {140, 24})
    answer(session, {:ok, %{page() | rows: [], cursor: nil}})

    for key <- ["f", "g"] do
      Test.input(session, key)
      assert Test.render!(session) =~ "No record is selected"
    end

    screen = Test.render!(session)
    assert screen =~ "This page has no records."
    refute screen =~ "Select a record"
    refute screen =~ "Enter Details"
    refute screen =~ "c Content"

    session = start_view(request: [view: :log, opts: [base: "/log"]], size: {140, 24})
    answer(session, {:ok, page()})
    assert Test.render!(session) =~ "c Content"
  end

  test "automatic shard selection reports the switch and preserves the selected shard" do
    session = start_view(request: [view: :streams, shard: nil], refresh: 1000)
    assert_receive {:read, %{count_others: true}, worker}
    reply(session, worker, {:ok, Map.put(%{page() | shard: 1}, :moved_from, 0)})
    screen = Test.render!(session)
    assert screen =~ "Shard 0 has no rows; showing shard 1"
    assert screen =~ "shard 1"

    fire_refresh(session)
    assert_receive {:read, %{shard: 1, count_others: false}, _worker}
  end

  test "switching listings preserves the selected shard without recounting other shards" do
    session = start_view(request: [view: :streams, shard: nil])
    assert_receive {:read, %{count_others: true}, worker}
    reply(session, worker, {:ok, %{page() | shard: 1}})
    Test.input(session, "1")
    assert_receive {:read, %{view: :raw, shard: 1, count_others: false}, _worker}
  end

  test "an explicitly selected shard does not trigger counts on other shards" do
    session = start_view(request: [view: :streams, shard: 2])
    assert_receive {:read, %{count_others: false}, worker}
    reply(session, worker, {:ok, %{page() | rows: [], cursor: nil, shard: 2}})
    assert Test.render!(session) =~ "No rows here. s selects another shard."
  end

  test "a completed shard listing preserves an active command edit" do
    session = start()
    Test.input(session, "s")
    Test.input(session, "/")
    Test.input(session, "\x15")
    type(session, "draft")
    answer(session, {:ok, [%{shard: 0, owner: nil, status: nil}]})
    assert Test.render!(session) =~ "Command: draft_"
    Test.input(session, "Escape")
    assert Test.render!(session) =~ "Select shard"
    Test.input(session, "Escape")
    assert Test.render!(session) =~ "Query: scan"
  end

  test "Yjs text has real line breaks, wraps and escapes terminal controls" do
    session = start()
    text = "First line\nSecond line " <> String.duplicate("界", 50) <> "\e[31m"

    data = %{
      owner: :slap@localhost,
      shard: 0,
      durable_seq: nil,
      label: "Document content",
      content: {:json, JSON.encode!(%{roots: [%{name: "body", value: text}]})}
    }

    Test.input(session, "r")
    answer(session, {:ok, data})
    Test.input(session, "Enter")
    show(session, :text)
    screen = Test.render!(session)
    assert screen =~ "First line"
    assert screen =~ "Second line"
    assert screen =~ ~S(\u{1B}[31m)
    refute screen =~ "durable seq"
    refute screen =~ "First line\\nSecond"
  end

  defp start(overrides \\ []) do
    value = overrides[:value] || "abc"

    data = %{
      page()
      | rows: [%{row() | value: value, value_size: overrides[:total] || byte_size(value)}]
    }

    view = overrides[:view] || :raw
    opts = overrides[:opts] || if(view == :object, do: [object_key: "a"], else: [])
    session = start_view(request: [view: view, opts: opts])
    answer(session, {:ok, data})
    session
  end

  defp page, do: %{owner: :slap@localhost, shard: 0, durable_seq: 4, cursor: nil, rows: [row()]}
  defp row, do: %{key: "a", value: "abc", offset: 0, value_size: 3, oversized_key: false}
end
