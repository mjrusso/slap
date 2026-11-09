defmodule Slap.Inspect.ViewContentTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias Breeze.Test

  @capture %{owner: :slap@localhost, encoded_bytes: 2_000_000}
  @grown %{@capture | encoded_bytes: 2_500_000}

  test "content waits for confirmation and cancellation does not reconstruct" do
    capture = %{@capture | encoded_bytes: 3_000_000}

    session =
      start_view(request: [view: :yjs_content, opts: [base: "/doc", roots: [{"body", :text}]]])

    assert_receive {:read, %{opts: preflight}, worker}
    assert preflight[:preflight]
    edit(session, "")
    reply(session, worker, {:ok, capture})
    Test.input(session, "y")
    editing = Test.render!(session)
    assert editing =~ "Command: y_"
    refute editing =~ "3000000 wire bytes"
    Test.input(session, "Escape")
    screen = Test.render!(session)
    assert screen =~ "3000000 wire bytes"
    assert screen =~ "y Confirm"
    refute screen =~ "n/p Page"
    Test.input(session, "n")
    refute Test.render!(session) =~ "y Confirm"
    refute_receive {:read, _, _}
    Test.input(session, "r")
    answer(session, {:ok, capture})
    Test.input(session, "y")
    assert_receive {:read, %{opts: confirmed}, _worker}
    refute confirmed[:preflight]
    assert confirmed[:roots] == [{"body", :text}]
  end

  test "log content returns entries with local byte paging" do
    session = start_view(request: [view: :log, opts: [base: "/log"]])
    answer(session, {:ok, page()})
    Test.input(session, "c")
    assert %{view: :log_content, opts: opts} = answer(session, {:ok, @capture})
    assert opts[:preflight]
    Test.input(session, "y")
    large = String.duplicate("a", 5000)

    rows = [
      %{
        key: "entry 7",
        value: large,
        value_size: 5000,
        oversized_key: false,
        decoded: {:ok, %{kind: :entry, key: "offset 7"}}
      }
    ]

    assert %{view: :log_content, opts: opts} =
             answer(session, {:ok, %{owner: :slap@localhost, shard: 0, cursor: nil, rows: rows}})

    refute opts[:preflight]
    show(session, :bytes)
    assert Test.render!(session) =~ "Details · bytes · Focused"
    assert Test.render!(session) =~ "Bytes 0–4095 of 5000"
    Test.input(session, "m")
    refute_receive {:read, _, _}
    assert Test.render!(session) =~ "Bytes 4096–4999 of 5000"
  end

  test "small or previously confirmed content skips confirmation" do
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc", roots: []]])
    assert %{opts: opts} = answer(session, {:ok, %{@capture | encoded_bytes: 1024}})
    assert opts[:preflight]
    assert %{opts: opts} = answer(session, {:ok, root_page([], ["body"])})
    refute opts[:preflight]

    session = root_session([], ["body"])
    Test.input(session, "i")
    Test.input(session, "ArrowRight")
    Test.input(session, "Enter")
    assert %{opts: opts} = answer(session, {:ok, @capture})
    assert opts[:preflight]
    assert %{opts: opts} = answer(session, {:ok, root_page([{"body", :text}], ["body"])})
    refute opts[:preflight]
    assert opts[:roots] == [{"body", :text}]
    refute Test.render!(session) =~ "y Confirm"
  end

  test "a confirmation that arrives under help is shown when help closes" do
    session =
      start_view(request: [view: :yjs_content, opts: [base: "/doc", roots: [{"body", :text}]]])

    assert_receive {:read, %{opts: preflight}, worker}
    assert preflight[:preflight]
    Test.input(session, "?")
    reply(session, worker, {:ok, @capture})
    Test.input(session, "y")
    refute Test.render!(session) =~ "y Confirm"
    refute_receive {:read, _, _}
    Test.input(session, "Escape")
    assert Test.render!(session) =~ "2000000 wire bytes"
    Test.input(session, "y")
    assert_receive {:read, %{opts: confirmed}, _worker}
    refute confirmed[:preflight]
  end

  test "unavailable history offers current content before any page is loaded" do
    opts = [base: "/history", at_offset: 10, roots: [{"body", :text}]]
    session = start_view(request: [view: :yjs_content, shard: nil, opts: opts])
    answer(session, {:error, {:history_unavailable, 42}})
    assert Test.render!(session) =~ "Press c for current content"
    Test.input(session, "c")
    assert_receive {:read, %{opts: current}, _worker}
    refute current[:at_offset]
    assert current[:roots] == [{"body", :text}]
    assert current[:preflight]
  end

  test "root interpretations use preflight and confirmation at 80 columns" do
    names = ["body", "items", "meta", "xml"]
    session = root_session([], names)
    screen = Test.render!(session)

    for hint <- ["i Roots", "q Quit"] do
      assert screen =~ hint
      footer = screen |> lines() |> Enum.find(&String.contains?(&1, hint))
      assert String.length(footer) <= 80
    end

    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    Test.render!(session)
    Test.input(session, "i")
    assert Test.render!(session) =~ "Interpret roots"

    for count <- 1..4 do
      for _ <- 1..count, do: Test.input(session, "ArrowRight")
      if count < 4, do: Test.input(session, "ArrowDown")
    end

    screen = Test.render!(session)
    for type <- ["text", "array", "map", "xml_fragment"], do: assert(screen =~ type)
    assert Enum.count_until(String.split(String.trim_trailing(screen), "\n"), 25) <= 24
    fire_refresh(session)
    refute_receive {:read, _, _}
    Test.input(session, "Enter")
    roots = [{"body", :text}, {"items", :array}, {"meta", :map}, {"xml", :xml_fragment}]
    assert %{opts: opts} = answer(session, {:ok, @grown})
    assert opts[:roots] == roots
    assert opts[:preflight]
    screen = Test.render!(session)
    assert screen =~ "y Confirm"
    # The displayed query changes only once the reconstruction is confirmed.
    refute query_line(screen) =~ "--root"
    Test.input(session, "y")
    assert %{opts: opts} = answer(session, {:ok, root_page(roots, names)})
    assert opts[:roots] == roots
    refute opts[:preflight]
    show(session, :term)
    assert Test.render!(session) =~ ~s("interpretation" => "text")
    Test.input(session, "i")
    assert Test.render!(session) =~ ~r/text\s+"body"/
  end

  test "text mode labels root interpretations when the query header is clipped" do
    name = String.duplicate("long-root-", 10)
    session = root_session([{name, :text}, {"body", :xml_fragment}], [name, "body"])
    Test.input(session, "Enter")
    show(session, :text)
    screen = Test.render!(session)
    refute screen |> lines() |> Enum.at(1) =~ "xml_fragment"
    assert screen =~ "(text)"
    assert screen =~ "body (xml_fragment)"
  end

  test "text mode explains root type mismatches" do
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc"]])
    answer(session, {:ok, @capture})
    Test.input(session, "y")
    root = %{name: "body", interpretation: :text, status: :wrong_type, holds: :map}

    answer(
      session,
      {:ok, %{root_page([], []) | content: {:json, JSON.encode!(%{roots: [root]})}}}
    )

    Test.input(session, "Enter")
    show(session, :text)

    assert Test.render!(session) =~ "Cannot display as text: this root contains map entries."
  end

  test "ambiguous sequence content is not labelled as a wrong type" do
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc"]])
    answer(session, {:ok, @capture})
    Test.input(session, "y")
    root = %{name: "body", interpretation: :text, status: :ambiguous, holds: :sequence}
    page = %{root_page([], []) | content: {:json, JSON.encode!(%{roots: [root]})}}
    answer(session, {:ok, page})
    Test.input(session, "Enter")
    show(session, :text)
    screen = Test.render!(session)
    assert screen =~ "Type uncertain:"
    assert screen =~ "text containing only embeds"
    refute screen =~ "Cannot display as text:"
  end

  test "inferred root types are labelled and preselected in the dialog" do
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc"]], size: {120, 30})
    answer(session, {:ok, @capture})
    Test.input(session, "y")

    roots = [
      %{name: "body", interpretation: :text, value: "hello", inferred: true},
      %{name: "list", status: :uninterpreted, type: :untyped, holds: :sequence},
      %{name: "meta", interpretation: :map, value: %{"k" => "v"}, inferred: true},
      %{name: "other", status: :uninterpreted, type: :untyped}
    ]

    answer(session, {:ok, %{root_page([], []) | content: {:json, JSON.encode!(%{roots: roots})}}})
    Test.input(session, "Enter")
    show(session, :text)
    screen = Test.render!(session)
    assert screen =~ "body (text, inferred)"
    assert screen =~ "meta (map, inferred)"
    assert screen =~ ~s("k": "v")
    refute screen =~ ~s("inferred": true)
    assert screen =~ "No root type selected. It contains a"
    Test.input(session, "Escape")
    Test.input(session, "i")
    screen = Test.render!(session)
    assert screen =~ ~r/> text +"body"/
    assert screen =~ ~r/  none +"list"  \(holds an array, XML or text containing only embeds\)/
    assert screen =~ ~r/  map +"meta"/
    assert screen =~ ~r/  none +"other"/
    Test.input(session, "Enter")
    assert_receive {:read, %{opts: opts}, _worker}
    assert opts[:roots] == [{"body", :text}, {"meta", :map}]
  end

  test "Yjs content stays manual when timed refresh is enabled" do
    session = root_session([], ["body"], 30_000)
    assert Test.render!(session) =~ "Manual read"
    fire_refresh(session)
    refute_receive {:read, _, _}
    Test.input(session, "t")
    Test.input(session, "t")
    fire_refresh(session)
    refute_receive {:read, _, _}
    Test.input(session, "r")
    assert_receive {:read, %{opts: opts}, _worker}
    assert opts[:preflight]
  end

  test "root chooser preserves CLI choices on cancellation and scrolls a long root list" do
    names = ["body" | Enum.map(1..20, &"root#{&1}")]
    session = root_session([{"body", :map}], names)
    before = Test.render!(session)
    Test.input(session, "i")
    assert Test.render!(session) =~ ~r/> map\s+"body"/
    for _ <- 1..20, do: Test.input(session, "ArrowDown")
    Test.input(session, "ArrowLeft")
    screen = Test.render!(session)
    assert screen =~ ~r/> xml_fragment\s+"root20"/
    assert screen =~ ~r/  none\s+"root19"/
    Test.input(session, "Escape")
    refute_receive {:read, _, _}
    assert query_line(Test.render!(session)) == query_line(before)
    Test.input(session, "i")
    assert Test.render!(session) =~ ~r/map\s+"body"/
    for _ <- 1..3, do: Test.input(session, "ArrowLeft")
    Test.input(session, "Enter")
    assert_receive {:read, %{opts: opts}, _worker}
    assert opts[:roots] == []
    assert opts[:preflight]
  end

  test "cancelling root confirmation preserves the displayed interpretations" do
    for key <- ["n", "Escape"] do
      session = root_session([{"body", :map}], ["body"])
      before = Test.render!(session)
      Test.input(session, "i")
      Test.input(session, "ArrowRight")
      Test.input(session, "Enter")
      assert %{opts: opts} = answer(session, {:ok, @grown})
      assert opts[:roots] == [{"body", :xml_fragment}]
      Test.input(session, key)
      screen = Test.render!(session)
      assert screen =~ "Read cancelled"
      assert query_line(screen) == query_line(before)
      refute_receive {:read, _, _}
      Test.input(session, "i")
      assert Test.render!(session) =~ ~r/map\s+"body"/
      Test.input(session, "Escape")
      Test.input(session, "r")
      assert_receive {:read, %{opts: opts}, _worker}
      assert opts[:roots] == [{"body", :map}]
    end
  end

  test "root chooser waits for requests and reopens after errors" do
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc", roots: []]])
    answer(session, {:ok, @capture})
    Test.input(session, "y")
    answer(session, {:ok, root_page([], ["body"])})

    Test.input(session, "r")
    assert_receive {:read, _request, worker}
    Test.input(session, "i")
    assert Test.render!(session) =~ "Loading"
    refute Test.render!(session) =~ "Interpret roots"
    reply(session, worker, {:ok, @grown})
    assert Test.render!(session) =~ "y Confirm"
    refute Test.render!(session) =~ "Interpret roots"
    Test.input(session, "y")
    assert_receive {:read, _request, worker}
    Test.input(session, "i")
    refute Test.render!(session) =~ "Interpret roots"
    reply(session, worker, {:error, :busy})
    assert Test.render!(session) =~ "Press r to retry"
    Test.input(session, "i")
    assert Test.render!(session) =~ "Interpret roots"
    Test.input(session, "Escape")

    for reason <- [:busy, {:history_unavailable, 42}, :request_failed] do
      Test.input(session, "r")
      answer(session, {:error, reason})
      Test.input(session, "i")
      assert Test.render!(session) =~ "Interpret roots"
      Test.input(session, "Escape")
    end
  end

  test "cancelling content for another document does not restore the previous document" do
    session = root_session([], ["body"])
    edit(session, "yjs --content --base /other")
    Test.input(session, "Enter")
    assert %{opts: opts} = answer(session, {:ok, @capture})
    assert opts[:base] == "/other"
    Test.input(session, "n")
    Test.input(session, "i")
    screen = Test.render!(session)
    assert screen =~ "Load Yjs content to choose its roots"
    refute screen =~ "Interpret roots"
  end

  defp root_session(roots, names, refresh \\ 0) do
    request = [view: :yjs_content, opts: [base: "/doc", roots: roots]]
    session = start_view(refresh: refresh, request: request)
    assert %{opts: opts} = answer(session, {:ok, @capture})
    assert opts[:preflight]
    Test.input(session, "y")
    assert %{opts: opts} = answer(session, {:ok, root_page(roots, names)})
    refute opts[:preflight]
    session
  end

  defp root_page(choices, names) do
    choices = Map.new(choices)

    roots =
      Enum.map(names, fn name ->
        case choices[name] do
          nil -> %{name: name, status: :uninterpreted, type: :untyped}
          type -> %{name: name, interpretation: type, value: ""}
        end
      end)

    %{
      owner: :slap@localhost,
      shard: 0,
      label: "Document content",
      content: {:json, JSON.encode!(%{roots: roots})}
    }
  end

  defp query_line(screen), do: screen |> lines() |> Enum.find(&String.contains?(&1, "Query:"))
end
