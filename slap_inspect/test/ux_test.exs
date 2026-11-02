defmodule Slap.Inspect.UXTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase,
    only: [start_view: 1, answer: 2, reply: 3, fire_refresh: 1, type: 2]

  alias Breeze.Test

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
    assert Test.render!(session) =~ "00000000"
    Test.input(session, "r")
    answer(session, {:ok, %{page() | rows: []}})
    screen = Test.render!(session)
    assert screen =~ "Selected record is no longer present"
    assert screen =~ "No rows"
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "Records · Focused"
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
    Test.input(session, "d")
    assert Test.render!(session) =~ "Text"
    assert Test.render!(session) =~ "hello"
    Test.input(session, "d")
    screen = Test.render!(session)
    assert screen =~ "JSON"
    assert screen =~ ~s("message":)
    Test.input(session, "d")
    assert Test.render!(session) =~ "00000000"
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

  test "byte navigation and refresh preserve configured read limits" do
    for view <- [:raw, :object] do
      opts = [max_value_bytes: 64, max_key_bytes: 128, target_bytes: 512]
      opts = if view == :object, do: opts ++ [object_key: "a", instance: "files"], else: opts
      session = start(view: view, opts: opts, value: String.duplicate("x", 64), total: 256)
      Test.input(session, "Enter")
      Test.input(session, "m")
      assert_receive {:read, %{view: next_view, opts: next}, worker}
      assert next_view == if(view == :object, do: :object, else: :get)
      assert next[:max_value_bytes] == 64
      assert next[:max_key_bytes] == 128
      assert next[:target_bytes] == 512
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
      assert screen =~ "Records · Focused"
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

  test "long query arguments use the available header width" do
    session = start(view: :object, opts: [object_key: "objects/" <> String.duplicate("x", 160)])
    assert session |> Test.render!() |> String.split("\n") |> Enum.at(1) =~ "objects/"
  end

  test "long record labels keep the selected row visible" do
    session = start()
    rows = for index <- 1..30, do: %{row() | key: "row#{index}-" <> String.duplicate("x", 160)}
    Test.input(session, "r")
    answer(session, {:ok, %{page() | rows: rows}})
    for _ <- 1..15, do: Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ ~s(15 "row15-)
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "row15-"
  end

  test "view shortcuts request missing arguments without starting a read" do
    for {key, status, command} <- [
          {"5", "Required: --base", "log --shard 0 --base _"},
          {"6", "Required: --base or --sid", "yjs --shard 0 --base _"}
        ] do
      session = start(view: :kv, opts: [partition: "users", key_prefix: <<0>>])
      Test.input(session, key)
      screen = Test.render!(session)
      assert screen =~ status
      assert screen =~ "Filter command: " <> command
      refute screen =~ "--partition users_"
      refute_receive {:read, _, _}
      Test.input(session, "Escape")
      assert Test.render!(session) =~ "kv --shard 0 --partition users"
    end
  end

  test "a view shortcut prefills the missing option, and Enter reads the value typed after it" do
    session = start(view: :kv, opts: [partition: "users"])
    Test.input(session, "6")
    for key <- String.graphemes("/docs/a"), do: Test.input(session, key)
    Test.input(session, "Enter")
    assert_receive {:read, %{view: :yjs, opts: opts}, _worker}
    assert opts[:base] == "/docs/a"
    refute Test.render!(session) =~ "Required:"
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

  test "Tab changes the focused pane and its keyboard help" do
    session = start()
    Test.render!(session)
    Test.input(session, "\t")
    terminal = %{session.terminal | size: %{width: 60, height: 18}}
    screen = Test.render!(session, terminal: terminal)
    assert screen =~ "Focused"
    assert screen =~ "Esc List"
    Test.input(session, "\t")
    assert Test.render!(session) =~ "Records · Focused"
  end

  test "shard selection shows available shards and clears the old page cursor" do
    session = start(opts: [cursor: <<0>>, cursor_exclusive: true])
    Test.input(session, "s")
    answer(session, {:ok, [0, 1, 2]})
    assert Test.render!(session) =~ "Select shard"
    assert Test.render!(session) =~ "> 0"
    Test.input(session, "ArrowDown")
    assert Test.render!(session) =~ "> 1"
    Test.input(session, "Enter")
    assert_receive {:read, %{shard: 1, opts: opts}, worker}
    refute opts[:cursor]
    refute opts[:cursor_exclusive]
    reply(session, worker, {:ok, %{page() | shard: 1}})
    assert Test.render!(session) =~ "shard 1"
  end

  test "a completed shard listing preserves an active command edit" do
    session = start()
    Test.input(session, "s")
    Test.input(session, "/")
    Test.input(session, "\x15")
    type(session, "draft")
    answer(session, {:ok, [0, 1]})
    assert Test.render!(session) =~ "Filter command: draft_"
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
    Test.input(session, "d")
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
