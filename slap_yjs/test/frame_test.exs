defmodule Slap.Yjs.FrameTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Slap.Yjs.Frame

  test "varuints as lib0 writes them" do
    assert Frame.varuint(0) == <<0>>
    assert Frame.varuint(127) == <<127>>
    assert Frame.varuint(128) == <<128, 1>>
    assert Frame.varuint(300) == <<172, 2>>
    assert Frame.varuint(16_384) == <<128, 128, 1>>
    assert Frame.read_varuint(<<172, 2, "rest">>) == {:ok, 300, "rest"}
  end

  test "a frame is the update's size, then the update" do
    assert Frame.frame("abc") == <<3, "abc">>
    assert Frame.frame(:binary.copy("x", 200)) == <<200, 1>> <> :binary.copy("x", 200)
    assert Frame.frames(["a", "", "bc"]) == <<1, "a", 0, 2, "bc">>
  end

  test "truncated and oversized input is an error" do
    assert Frame.parse(<<3, "ab">>) == {:error, :truncated}
    assert Frame.parse(<<128>>) == {:error, :truncated}
    assert Frame.parse(:binary.copy(<<255>>, 9) <> <<1>>) == {:error, :invalid}
  end

  test "varuints stop at lib0's limit, 2^53 - 1" do
    max = 2 ** 53 - 1
    assert Frame.read_varuint(Frame.varuint(max)) == {:ok, max, ""}
    assert Frame.read_varuint(Frame.varuint(max + 1)) == {:error, :invalid}
    assert Frame.read_varuint(Frame.varuint(2 ** 56 - 1)) == {:error, :invalid}
  end

  property "parse(concatenated frames) gives back the updates" do
    check all(updates <- list_of(update(), min_length: 1, max_length: 9), max_runs: 500) do
      assert Frame.parse(Frame.frames(updates)) == {:ok, updates}

      {a, b} = Enum.split(updates, div(length(updates), 2))
      assert Frame.parse(Frame.frames(a) <> Frame.frames(b)) == {:ok, updates}
    end
  end

  # Mostly small, sometimes across the one- and two-byte length boundaries.
  defp update do
    [member_of([0, 1, 127, 128, 129, 16_383, 16_384]), integer(0..300)]
    |> one_of()
    |> bind(&binary(length: &1))
  end
end
