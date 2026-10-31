defmodule Slap.Inspect.ViewDetailsTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias Breeze.Test

  test "detail scrolling survives refresh but resets when selecting another record" do
    first = %{row() | value: String.duplicate("x", 512), value_size: 512}
    first = Map.put(first, :decoded, {:ok, %{note: String.duplicate("long", 60)}})
    data = %{page() | rows: [first, %{row() | key: "b"}]}
    session = start_view(size: {140, 36})
    answer(session, {:ok, data})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
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
    assert screen =~ ~s(key: "b")
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
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ ~S(key: "accent-e\u{301}")
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
    assert screen =~ "storage: :object"
    assert screen =~ ~s(content_type: "text/plain")
  end

  test "partly read legacy indexes have a visible label" do
    data =
      Map.put(page(), :log, %{recognition: %{kind: :snapshot_log, legacy: true, index: :partial}})

    session = start_view()
    answer(session, {:ok, data})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "legacy snapshot log (index partly read)"
  end

  test "focused details scroll vertically and expose trailing hex bytes" do
    value = :binary.copy(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 254, 255>>, 32)
    first = %{row() | value: value, value_size: byte_size(value)}
    session = start_view(size: {110, 24})
    answer(session, {:ok, %{page() | rows: [first, %{first | key: "b"}]}})
    Test.input(session, "ArrowDown")
    Test.render!(session)
    Test.input(session, "\t")
    before = Test.render!(session)
    assert before =~ "Details · bytes · Focused"
    Test.input(session, "PageDown")
    vertical = Test.render!(session)
    assert vertical != before
    assert vertical =~ "000000"
    refute vertical =~ "FE FF"
    for _ <- 1..60, do: Test.input(session, "ArrowRight")
    assert Test.render!(session) =~ "FE FF"
    for _ <- 1..60, do: Test.input(session, "ArrowLeft")
    assert Test.render!(session) == vertical
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
    assert screen =~ "offset: 4"
    Test.input(session, "m")
    screen = Test.render!(session)
    assert screen =~ "End of value"
    assert screen =~ "00000004"
    refute_receive {:read, _, _}
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
      Test.input(session, "ArrowDown")
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
      screen = Test.render!(session)
      assert screen =~ ~s(1 raw "a")
      assert screen =~ ~s(2 raw "b")
      # A preview of "a" applied to "b" would show its key in the details.
      assert screen =~ ~s(key: "b")
    end
  end
end
