defmodule Slap.Inspect.MessagesTest do
  use ExUnit.Case, async: true

  alias Slap.Inspect.Messages

  test "presentation uses server validity and logical boundaries without decoding rows" do
    page = page(["a", "bc"], %{fragment(:complete) | row_count: 2, next_offset: 3})

    assert {:ok, [%{value: "abc", complete: true, next_offset: 3}]} = Messages.assemble(page)

    invalid = page(["abc"], fragment(:invalid))

    assert {:ok, [%{complete: false, diagnostic: :incomplete_message_parts}]} =
             Messages.assemble(invalid)

    assert {:error, :incomplete_message_parts} = Messages.accumulate(invalid, nil)
  end

  test "collection joins fragments through opaque server tokens" do
    token = <<1, 9, 8>>
    first = page(["ab"], %{fragment(:more) | after: token})
    last = page(["c"], %{fragment(:complete) | before: token})
    assert {:ok, [], pending} = Messages.accumulate(first, nil)
    assert {:ok, [%{value: "abc", complete: true}], nil} = Messages.accumulate(last, pending)

    assert {:ok, [%{complete: false}]} = Messages.assemble(last)
    assert {:error, :incomplete_message_parts} = Messages.accumulate(last, nil)

    wrong = page(["c"], %{fragment(:complete) | before: <<2>>})
    assert {:error, :incomplete_message_parts} = Messages.accumulate(wrong, pending)
  end

  test "fragment descriptors must cover exactly the received rows" do
    for count <- [-1, 0, 2] do
      malformed = page(["abc"], %{fragment(:complete) | row_count: count})
      assert {:error, :invalid_message_fragments} = Messages.assemble(malformed)
    end

    assert {:error, :invalid_message_fragments} =
             Messages.assemble(page(["a", "bc"], fragment(:complete)))
  end

  test "collection preserves message order within a page" do
    page = %{
      fragment_contract: 1,
      rows: [%{value: "first"}, %{value: "second"}],
      fragments: [fragment(:complete), %{fragment(:complete) | offset: 7, next_offset: 14}]
    }

    assert {:ok, [%{value: "first"}, %{value: "second"}], nil} = Messages.accumulate(page, nil)
  end

  test "unsupported fragment contracts do not fall back to stored-message inference" do
    assert {:error, :unsupported_message_fragments} = Messages.assemble(%{rows: []})

    assert {:error, :unsupported_message_fragments} =
             Messages.accumulate(%{fragment_contract: 2, rows: [], fragments: []}, nil)
  end

  defp page(values, fragment),
    do: %{fragment_contract: 1, fragments: [fragment], rows: Enum.map(values, &%{value: &1})}

  defp fragment(status) do
    %{
      offset: 0,
      row_count: 1,
      next_offset: 7,
      encoded_bytes: 7,
      before: nil,
      after: nil,
      status: status
    }
  end
end
