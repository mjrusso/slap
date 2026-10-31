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
    assert Test.render!(session) =~ ~s(key: "a")
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
    Test.input(session, "Escape")

    for cursor <- ["c", "b", nil] do
      assert %{view: :raw, opts: opts} = answer(session, linked.(nil))
      assert opts[:cursor] == cursor
      if cursor, do: Test.input(session, "p")
    end
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

    Test.input(session, "ArrowDown")
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

  test "following an object preserves its Files instance" do
    file = %{row() | decoded: {:ok, %{value: %{body: [:object, "object-key"]}}}}
    session = start_view(request: [opts: [instance: NamedFiles]])
    answer(session, {:ok, %{page() | rows: [file]}})
    Test.input(session, "ArrowDown")
    Test.input(session, "f")
    assert_receive {:read, %{view: :object, opts: opts}, _worker}
    assert opts[:instance] == NamedFiles
    assert opts[:object_key] == "object-key"
  end
end
