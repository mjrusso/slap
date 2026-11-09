defmodule Slap.Inspect.ViewDetailsTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias Breeze.Test
  alias Slap.Inspect.{Details, Labels}

  test "text wraps at spaces and splits only words wider than the pane" do
    row = %{row() | value: "alpha beta gamma abcdefghijklmnop", value_size: 33}
    {:text, text} = Details.render(row, nil, :text, 12)
    body = text |> String.split("Text (wrapped)\n") |> List.last() |> String.split("\n")
    assert body == ["alpha beta ", "gamma ", "abcdefghijkl", "mnop"]
  end

  test "JSON keeps each object's stored key order and repeated keys" do
    value = ~s({"zeta": 1, "alpha": {"y": [2], "x": null}, "zeta": 3})
    row = %{row() | value: value, value_size: byte_size(value)}
    {:json, text} = Details.render(row, nil, :json, 80)
    assert text =~ ~r/"zeta": 1,\s+"alpha": \{\s+"y": \[\s+2\s+\],\s+"x": null\s+\},\s+"zeta": 3/
  end

  test "logical summaries show fork metadata and the requested offset range" do
    meta = %{
      decoded: {:ok, %{kind: :meta, path: "/fork", value: %{sid: 2, content_type: "text/plain"}}}
    }

    source = %{path: "/source", sid: 1, shard: 0}

    overview = %{
      metadata: %{
        row: meta,
        shard: 0,
        logical: %{from: 0, until: 22, reference: %{source: source, offset: 11, depth: 1}},
        records: %{tail: {:ok, %{row: %{decoded: {:ok, %{value: %{next_offset: 22}}}}}}}
      },
      # This page contains source records; the fork's tail is in its metadata.
      tail: %{decoded: {:ok, %{value: %{next_offset: 9}}}},
      interpretation: :logical
    }

    row = %{key: <<>>, value: <<>>, decoded: {:ok, %{kind: :overview, value: overview}}}
    {:summary, summary} = Details.render(row, nil, :text, 100)
    refute summary =~ "%{"
    assert summary =~ ~r/^Stream +\/fork$/m
    assert summary =~ ~r/^Fork of +\/source \(sid 1, shard 0\)$/m
    assert summary =~ ~r/^Inherits +offsets before 11$/m
    refute summary =~ "Tail"
    assert summary =~ ~r/^Offsets read +0 to 22$/m
  end

  test "stream metadata defaults to a summary and supports byte and term views" do
    meta = %{
      row()
      | key: <<1, ?/, ?s>>,
        value: :erlang.term_to_binary(%{sid: 7}),
        value_size: 20,
        decoded: {:ok, %{kind: :meta, path: "/s", value: %{sid: 7, closed: false}}}
    }

    session = start_view(size: {140, 30})
    answer(session, {:ok, %{page() | rows: [meta]}})
    Test.input(session, "Enter")
    screen = Test.render!(session)
    assert screen =~ "Details · summary"
    assert screen =~ ~r/Path +\/s/
    refute screen =~ "00000000"
    refute screen =~ "No UTF-8 text"

    for {mode, shown} <- [bytes: "00000000  ", term: "decoded: {:ok,", summary: "Path"] do
      Test.input(session, "d")
      screen = Test.render!(session)
      assert screen =~ "Details · #{mode}"
      assert screen =~ shown
    end
  end

  test "listed logs, Yjs documents and home rows show only their summary" do
    meta = :erlang.term_to_binary(%{sid: 1})

    for decoded <- [
          %{kind: :log, base: "/log"},
          %{kind: :yjs_document, base: "/d", service: "s", name: "n"},
          %{kind: :cluster_view, cluster: Demo, view: :streams, shards: 1, counts: nil}
        ] do
      value = if decoded.kind == :cluster_view, do: "", else: meta
      row = %{row() | value: value, value_size: byte_size(value), decoded: {:ok, decoded}}
      assert {:summary, text} = Details.render(row, nil, :text, 80)
      refute text =~ "00000000"
      refute text =~ "%{"
    end
  end

  test "an empty value shows its fields without a note about missing text" do
    row = %{row() | value: "", value_size: 0, decoded: {:ok, %{kind: :raw, note: "fields"}}}
    {:term, text} = Details.render(row, nil, :text, 80)
    assert text =~ ~s(note: "fields")
    refute text =~ "No UTF-8 text"
  end

  test "summaries align non-nil decoded fields; term mode shows the raw record" do
    meta = %{
      kind: :meta,
      path: "/s",
      value: %{sid: 1, registration_count: 2, ttl_s: 5, expires_at_ms: 0, closed_by: nil}
    }

    file = %{
      family: :record,
      id: "f",
      value: %{
        size: 1,
        storage: :inline,
        body: [:inline, %{}],
        sha256: <<1>>,
        metadata: %{"tick" => "68"}
      }
    }

    for {decoded, lines} <- [
          {meta,
           [
             "Path                /s",
             "Registration count  2",
             "Ttl (s)             5",
             "Expires at          1970-01-01T00:00:00.000Z"
           ]},
          {file, ["Metadata tick  68"]}
        ] do
      row = %{row() | decoded: {:ok, decoded}}
      {:bytes, bytes} = Details.render(row, nil, :bytes, 80)
      for line <- lines, do: assert(bytes =~ line)
      refute bytes =~ "decoded:"
      refute bytes =~ "Closed by"
      {:term, term} = Details.render(row, nil, :term, 80)
      assert term =~ "decoded: {:ok,"
    end
  end

  test "summaries display decoded numeric values" do
    trim = %{
      row()
      | value: <<128::64>>,
        value_size: 8,
        decoded: {:ok, %{kind: :trim, sid: 1, value: 128}}
    }

    assert {:summary, text} = Details.render(trim, nil, :text, 80)
    assert text =~ ~r/Decoded\s+128/
  end

  test "timestamps outside the calendar range display as integer milliseconds" do
    ms = 400_000_000_000_000_000
    meta = %{kind: :meta, path: "/s", value: %{sid: 1, expires_at_ms: ms, created_ms: ms}}
    {_mode, text} = Details.render(%{row() | decoded: {:ok, meta}}, nil, :text, 80)
    assert text =~ "Expires at  #{ms} ms"
    assert text =~ "Created     #{ms} ms"
  end

  test "text pagination preserves characters split across ranges" do
    value = String.duplicate("a", 4095) <> "界"
    row = %{row() | value: value, value_size: 8000}
    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: [row]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "m")
    assert_receive {:read, %{opts: opts}, _worker}
    assert opts[:value_range] == {4095, 4096}
  end

  test "a file label leaves out a missing content type" do
    file = %{family: :record, id: "f", value: %{size: 120, storage: :inline}}
    assert Labels.label(%{row() | decoded: {:ok, file}}) == "f · 120 B · inline"
  end

  test "text ranges and previews hide partial UTF-8 characters" do
    # A 3-byte "界" crosses the start of the range and the end of the loaded bytes.
    preview = %{offset: 2, value: <<0x8C, "ab", 0xE7, 0x95>>, value_size: 20}
    {:text, text} = Details.render(row(), preview, :text, 40)
    assert text =~ "Split characters hidden"
    assert text |> String.split("Text (wrapped)\n") |> List.last() == "ab"

    value = String.duplicate("a", 59) <> "界"
    message = %{kind: :message, offset: 1, part: 0}
    label = Labels.label(%{row() | value: value, value_size: 62, decoded: {:ok, message}})
    assert String.ends_with?(label, " · " <> String.duplicate("a", 59))
  end

  test "details start with a summary in every view, including the stream's content type" do
    meta = %{
      decoded:
        {:ok, %{kind: :meta, path: "/s", value: %{sid: 1, content_type: "application/json"}}}
    }

    message = %{kind: :message, offset: 32, part: 0, sid: 1}
    row = Map.merge(row(), %{seq: 6, value: ~s({"n":1}), value_size: 7, decoded: {:ok, message}})
    session = start_view(size: {140, 36})
    answer(session, {:ok, Map.merge(page(), %{rows: [row], metadata: %{row: meta}})})
    Test.input(session, "ArrowDown")
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")

    for _mode <- [:bytes, :text, :json] do
      screen = Test.render!(session)
      assert screen =~ "Offset        0000000000000000_0000000000000032"
      assert screen =~ "Content type  application/json"
      assert screen =~ "Seq           6"
      Test.input(session, "d")
    end
  end

  test "term details sort keys and truncate non-UTF-8 binaries to 32 bytes" do
    hash = <<0xCB, 0x01, 0xAD>>

    file = %{
      family: :record,
      id: "a.txt",
      value: %{sha256: hash, size: 1, storage: :inline, body: [:inline, %{bytes: 1}]},
      stored: :binary.copy(<<0xFF>>, 40)
    }

    row = Map.merge(row(), %{seq: 9, decoded: {:ok, file}})
    session = start_view(size: {240, 36})
    answer(session, {:ok, %{page() | rows: [row]}})
    Test.input(session, "Enter")
    show(session, :term)
    screen = Test.render!(session)
    assert screen =~ "sha256: 0xcb01ad"
    assert screen =~ "stored: 0x" <> String.duplicate("f", 64) <> "…"

    positions = for key <- ~w(decoded: key: seq: value_size:), do: :binary.match(screen, key)
    assert positions == Enum.sort(positions)
  end

  test "record labels identify messages, files and KV rows" do
    decoded = fn fields, decoded ->
      Map.merge(row(), Map.put(fields, :decoded, {:ok, decoded}))
    end

    rows = [
      decoded.(%{key: "m1", value: ~s({"tick":\n 1}), value_size: 11}, %{
        kind: :message,
        offset: 32,
        part: 0
      }),
      decoded.(%{key: "m2", value: <<0, 255>>, value_size: 2}, %{
        kind: :message,
        offset: 96,
        part: 1
      }),
      decoded.(%{key: "f1"}, %{
        family: :record,
        id: "report.txt",
        value: %{size: 24_638, content_type: "text/plain", storage: :object}
      }),
      decoded.(%{key: "f2"}, %{
        family: :intent,
        id: <<3, 23>>,
        value: %{ref: ["demo", "report.txt"]}
      }),
      decoded.(%{key: "k1", value: "Ada  Lovelace", value_size: 13}, %{
        kind: :row,
        partition: "demo",
        key: "my key"
      }),
      decoded.(%{key: "m1"}, %{
        kind: :meta,
        path: "/s",
        value: %{content_type: "application/json", reference: %{depth: 1}, closed: true}
      })
    ]

    session = start_view(size: {200, 36})
    answer(session, {:ok, %{page() | rows: rows}})
    screen = Test.render!(session)
    assert screen =~ ~s(1 @32 · 11 B · {"tick": 1})
    assert screen =~ "2 @96 · part 1 · 2 B · 0x00ff"
    assert screen =~ "3 report.txt · 24638 B · text/plain · object"
    assert screen =~ "4 intent 0x0317 · demo/report.txt"
    assert screen =~ ~s(5 demo / "my key" · 13 B · Ada Lovelace)
    assert screen =~ "6 /s · application/json · fork · closed"
  end

  test "non-UTF-8 labels preserve readable prefixes and show the remaining bytes as hex" do
    rows = [
      %{row() | key: "id" <> <<0xDC, 0x9A, 0xFF>>},
      %{row() | key: <<0xFF, 0x00>>},
      %{row() | key: "ünicode\n"}
    ]

    session = start_view(size: {100, 24})
    answer(session, {:ok, %{page() | rows: rows}})
    screen = Test.render!(session)
    assert screen =~ ~s(1 raw "id" 0xdc9aff)
    assert screen =~ ~s(2 raw 0xff00)
    assert screen =~ ~S(3 raw "ünicode\n")
    refute screen =~ "ܚ"
  end

  test "detail scrolling survives refresh but resets when selecting another record" do
    first = %{row() | value: String.duplicate("x", 512), value_size: 512}
    first = Map.put(first, :decoded, {:ok, %{note: String.duplicate("long", 60)}})
    data = %{page() | rows: [first, %{row() | key: "b"}]}
    session = start_view(size: {140, 36})
    answer(session, {:ok, data})
    Test.input(session, "Enter")
    show(session, :bytes)
    before = Test.render!(session)
    for _ <- 1..70, do: Test.input(session, "ArrowRight")
    Test.input(session, "PageDown")
    scrolled = Test.render!(session)
    assert scrolled != before
    Test.input(session, "r")
    answer(session, {:ok, data})

    assert session |> Test.render!() |> lines() |> Enum.drop(4) ==
             scrolled |> lines() |> Enum.drop(4)

    Test.input(session, "Escape")
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    screen = Test.render!(session)
    assert screen =~ ~r/Key\s+"b"/
    assert screen =~ "00000000"
  end

  test "combining marks remain explicit in record labels and decoded details" do
    rows =
      for key <- ["accent-e", "accent-e" <> <<0xCC, 0x81>>, "accent-é", ~S(accent-e\u{301})] do
        %{row() | key: key, decoded: {:ok, %{kind: :row, partition: "p", key: key}}}
      end

    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: rows}})
    screen = Test.render!(session)
    assert screen =~ ~S(accent-e\u{301})
    assert screen =~ ~S(accent-e\\u{301})
    assert screen =~ "accent-é"
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    show(session, :term)
    assert Test.render!(session) =~ ~S(key: "accent-e\u{301}")
  end

  test "reconstructed content falls back from bytes to text" do
    json = JSON.encode!(%{roots: [%{name: "body", interpretation: :text, value: "hello"}]})
    content = %{label: "Document content", content: {:json, json}}
    row = %{row() | value: "", value_size: 0, decoded: {:ok, %{kind: :overview, value: content}}}
    assert {:text, text} = Details.render(row, nil, :bytes, 80)
    assert text =~ "body (text)\nhello"
  end

  test "a long content label wraps in the JSON and term views" do
    label =
      "Includes all updates before offset 1234 and may include later state from its snapshot"

    content = %{label: label, content: {:json, JSON.encode!(%{roots: []})}}
    row = %{row() | value: "", value_size: 0, decoded: {:ok, %{kind: :overview, value: content}}}

    for mode <- [:json, :term] do
      assert {^mode, text} = Details.render(row, nil, mode, 40)
      assert text =~ ~r/Includes all updates before offset 1234 ?\nand may include/
      refute text =~ "Bytes empty"
    end
  end

  test "reconstructed content shows combining marks in JSON names and values" do
    marked = "e" <> <<0xCC, 0x81>>

    data = %{
      owner: :slap@localhost,
      shard: 0,
      label: "Document content",
      content: {:json, JSON.encode!(%{marked => marked})}
    }

    session = start_view()
    answer(session, {:ok, data})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    show(session, :term)
    assert Test.render!(session) =~ ~S("e\u{301}" => "e\u{301}")
  end

  test "file details show decoded summaries beside binary storage keys" do
    file = %{
      row()
      | key: <<0, 31>> <> "files-ns:ZGVmYXVsdA:record:demoreport.txt",
        decoded:
          {:ok,
           %{
             id: "report.txt",
             value: %{storage: :object, size: 24_639, content_type: "text/plain"}
           }}
    }

    session = start_view(size: {140, 50})
    answer(session, {:ok, %{page() | rows: [file]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    screen = Test.render!(session)
    assert screen =~ ~r/Storage\s+object/
    assert screen =~ ~r/Content type\s+text\/plain/
  end

  test "partly read legacy indexes have a visible label" do
    data =
      Map.put(page(), :log, %{recognition: %{kind: :snapshot_log, legacy: true, index: :partial}})

    session = start_view()
    answer(session, {:ok, data})
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "legacy snapshot log (index partly read)"
  end

  test "focused details scroll vertically and expose trailing hex bytes" do
    value = :binary.copy(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 254, 255>>, 32)
    first = %{row() | value: value, value_size: byte_size(value)}
    session = start_view(size: {110, 24})
    answer(session, {:ok, %{page() | rows: [first, %{first | key: "b"}]}})
    Test.render!(session)
    Test.input(session, "\t")
    before = Test.render!(session)
    assert before =~ "Details · bytes · Focused"
    Test.input(session, "PageDown")
    vertical = Test.render!(session)
    assert vertical != before
    assert vertical =~ "000000"
    assert vertical =~ "0C0DFEFF"
    Test.input(session, "PageUp")
    assert Test.render!(session) == before
    Test.input(session, "End")
    Test.render!(session)
    Test.input(session, "ArrowDown")
    assert selected_label(Test.render!(session)) == ~s(1 raw "a")
  end

  test "byte paging displays the current offset and preserves the last range at EOF" do
    counter = %{
      key: "counter",
      value: <<0, 0, 0, 0>>,
      value_size: 8,
      offset: 0,
      oversized_key: false
    }

    first = page() |> Map.delete(:rows) |> Map.put(:row, counter)
    last = %{first | row: %{counter | value: <<0, 0, 3, 233>>, offset: 4}}

    session =
      start_view(
        size: {140, 36},
        request: [view: :get, opts: [key: "counter", value_range: {0, 4}]]
      )

    assert %{opts: [key: "counter", value_range: {0, 4}]} = answer(session, {:ok, first})
    Test.input(session, "ArrowDown")
    Test.input(session, "m")
    assert %{opts: opts} = answer(session, {:ok, last})
    assert opts[:value_range] == {4, 4096}
    screen = Test.render!(session)
    assert screen =~ "00000004"
    Test.input(session, "m")
    screen = Test.render!(session)
    assert screen =~ "End of value"
    assert screen =~ "00000004"
    refute_receive {:read, _, _}
  end

  test "complete values use local byte paging and reload on refresh" do
    row = fn byte ->
      %{
        key: "k",
        value: :binary.copy(<<byte>>, 5000),
        value_size: 5000,
        oversized_key: false,
        truncated: false
      }
    end

    session = start_view(size: {140, 36})
    answer(session, {:ok, %{page() | rows: [row.(1)]}})
    Test.input(session, "Enter")
    Test.input(session, "m")
    refute_receive {:read, _, _}
    assert Test.render!(session) =~ "Bytes 4096–4999 of 5000"

    Test.input(session, "r")
    assert %{view: :raw} = answer(session, {:ok, %{page() | rows: [row.(2)]}})
    screen = Test.render!(session)
    assert screen =~ "Bytes 4096–4999 of 5000"
    assert screen =~ "\\u{2}\\u{2}"

    Test.input(session, "r")
    short = %{row.(3) | value: :binary.copy(<<3>>, 100), value_size: 100}
    assert %{view: :raw} = answer(session, {:ok, %{page() | rows: [short]}})
    assert Test.render!(session) =~ "Bytes 0–99 of 100"
  end

  test "deadline-limited object byte previews are visibly partial" do
    data = %{
      owner: :slap@localhost,
      stopped: :end,
      object: %{key: "object", value: "da", value_size: 4, offset: 0}
    }

    partial = %{data | stopped: :deadline, object: %{data.object | value: "t", offset: 2}}

    session =
      start_view(request: [view: :object, opts: [object_key: "object", value_range: {0, 2}]])

    answer(session, {:ok, data})
    Test.input(session, "ArrowDown")
    Test.input(session, "m")
    answer(session, {:ok, partial})
    assert Test.render!(session) =~ "Partial: deadline reached"
  end

  describe "byte previews of a kv page on shard 1" do
    setup do
      first = %{row() | value_size: 8}

      session =
        start_view(size: {140, 36}, request: [view: :kv, shard: nil, opts: [partition: "p"]])

      answer(session, {:ok, %{page() | shard: 1, rows: [first, %{first | key: "b"}]}})
      Test.input(session, "m")
      assert_receive {:read, %{view: :get} = request, worker}
      %{session: session, request: request, worker: worker, first: first}
    end

    test "use the displayed page's resolved shard", %{request: request} do
      assert request.shard == 1
    end

    test "changing selection while bytes load preserves the record list", context do
      %{session: session, worker: worker, first: first} = context
      Test.input(session, "ArrowDown")
      reply(session, worker, {:ok, %{row: first}})
      show(session, :term)
      screen = Test.render!(session)
      assert screen =~ ~s(1 raw "a")
      assert screen =~ ~s(2 raw "b")
      # A preview of "a" applied to "b" would show its key in the details.
      assert screen =~ ~s(key: "b")
    end
  end
end
