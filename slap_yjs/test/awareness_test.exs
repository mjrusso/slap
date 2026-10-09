defmodule Slap.Yjs.AwarenessTest do
  use ExUnit.Case, async: true

  alias Slap.Yjs.Awareness

  test "decodes states and removals produced by y_ex" do
    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    id = Yex.Awareness.client_id(awareness)
    :ok = Yex.Awareness.set_local_state(awareness, %{"user" => %{"name" => "a"}})
    {:ok, update} = Yex.Awareness.encode_update(awareness)

    assert {:ok, [{^id, _clock, %{"user" => %{"name" => "a"}}}]} = Awareness.decode(update)

    :ok = Yex.Awareness.clean_local_state(awareness)
    {:ok, removal} = Yex.Awareness.encode_update(awareness, [id])
    assert {:ok, [{^id, _clock, nil}]} = Awareness.decode(removal)
  end

  test "round-trips entries and produces updates accepted by y_ex" do
    entries = [{7, 3, %{"user" => %{"name" => "a"}}}, {9, 1, nil}]
    assert Awareness.decode(Awareness.encode(entries)) == {:ok, entries}

    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    :ok = Yex.Awareness.apply_update(awareness, Awareness.encode([{7, 3, %{"name" => "a"}}]))
    assert Yex.Awareness.get_states(awareness) == %{7 => %{"name" => "a"}}
  end

  test "rejects malformed updates and trailing bytes" do
    for input <- ["", <<1>>, <<1, 5, 0, 3, "nul">>, <<1, 5, 0, 1, "{">>, <<0, "extra">>] do
      assert Awareness.decode(input) == {:error, :invalid}
    end

    assert Awareness.decode(<<0>>) == {:ok, []}
  end
end
