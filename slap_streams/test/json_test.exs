defmodule Slap.Streams.JsonTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Slap.Streams.Json

  test "splits one level of a top-level array, keeping the original bytes" do
    assert Json.split(~s({"event": "created"}), false) == {:ok, [~s({"event": "created"})]}

    assert Json.split(~s([{"event": "a"}, {"event": "b"}]), false) ==
             {:ok, [~s({"event": "a"}), ~s({"event": "b"})]}

    assert Json.split("[[1,2], [3,4]]", false) == {:ok, ["[1,2]", "[3,4]"]}
    assert Json.split("[[[1,2,3]]]", false) == {:ok, ["[[1,2,3]]"]}

    assert Json.split(~s| [ 1 ,\n{ "a" : [ 2 ] } , "x" ] |, false) ==
             {:ok, ["1", ~s({ "a" : [ 2 ] }), ~s("x")]}
  end

  test "strings with commas, brackets, quotes and escapes" do
    body = ~s(["a,b", "[x]", "q\\"uote", "back\\\\", {"k": "}"}])

    assert Json.split(body, false) ==
             {:ok, [~s("a,b"), ~s("[x]"), ~s("q\\"uote"), ~s("back\\\\"), ~s({"k": "}"})]}
  end

  test "empty arrays are allowed only on create" do
    assert Json.split("[]", true) == {:ok, []}
    assert Json.split(" [ ] ", false) == {:error, :empty_array}
  end

  test "invalid JSON" do
    for bad <- ["", "{", "[1,]", "nope", "[1] [2]"] do
      assert Json.split(bad, false) == {:error, :invalid_json}, bad
    end
  end

  property "split then join round-trips any array" do
    check all(values <- list_of(json(), min_length: 1, max_length: 6), max_runs: 1_000) do
      body = JSON.encode!(values)
      {:ok, parts} = Json.split(body, false)
      assert length(parts) == length(values)
      assert JSON.decode!(IO.iodata_to_binary(Json.join(parts))) == values
    end
  end

  # Any JSON value, with strings full of the characters that delimit JSON.
  defp json do
    string = one_of([string(:printable), member_of(["s,[]{}\"\\", ""])])
    leaf = one_of([integer(), float(), boolean(), constant(nil), string])

    tree(leaf, fn child ->
      pairs = list_of(tuple({string, child}), max_length: 4)
      one_of([list_of(child, max_length: 4), map(pairs, &Map.new/1)])
    end)
  end
end
