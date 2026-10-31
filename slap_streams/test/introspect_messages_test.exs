defmodule Slap.Streams.Introspect.MessagesTest do
  use ExUnit.Case, async: true

  alias Slap.Streams.Introspect.Messages

  test "annotates complete multipart messages at their physical boundaries" do
    rows = [row(0, 0, "a"), row(0, 1, "bc"), row(7, 0, "de")]
    page = Messages.annotate(page(rows, 13), &{:ok, &1})

    assert [
             %{interpretation: {:ok, "abc"}},
             continuation,
             %{interpretation: {:ok, "de"}}
           ] = page.rows

    refute Map.has_key?(continuation, :interpretation)
  end

  test "rejects incomplete parts, unreadable rows and incorrect wire boundaries" do
    for rows <- [
          [row(0, 1, "abc")],
          [row(0, 0, "a"), row(0, 2, "bc")],
          [row(0, 0, "ab")],
          [%{row(0, 0, "abc") | truncated: true}],
          [%{row(0, 0, "abc") | oversized_key: true}],
          [Map.delete(row(0, 0, "abc"), :decoded)]
        ] do
      page = Messages.annotate(page(rows, 7), &{:ok, &1})
      assert hd(page.rows).interpretation == {:error, :incomplete_message_parts}
    end
  end

  test "annotation limits do not restrict assembly of complete bodies" do
    body = String.duplicate("x", 3000)
    rows = [row(0, 0, body)]

    assert %{rows: [%{interpretation: {:ok, 3000}}]} =
             Messages.annotate(page(rows, 3004), &{:ok, byte_size(&1)})

    assert %{rows: [%{interpretation: {:error, :summary_too_large}}]} =
             Messages.annotate(page(rows, 3004), &{:ok, &1})

    page = Messages.annotate(%{page(rows, 3004) | hard_max_bytes: 0}, &{:ok, &1})
    assert page.interpretations_limited
    refute Map.has_key?(hd(page.rows), :interpretation)
  end

  test "fragment metadata remains within the declared response bound" do
    rows = for offset <- 0..199, do: row(offset * 4, 0, "")

    page =
      Map.merge(page(rows, 800), %{
        sid: 1,
        shard: 0,
        next_message_offset: 800,
        until: nil
      })

    page = %{page | hard_max_bytes: :erlang.external_size(page)}
    {page, nil} = Messages.fragments(page, nil)
    assert Enum.count_until(page.fragments, 201) == 200
    assert :erlang.external_size(page) <= page.hard_max_bytes
  end

  test "a page that stopped early ends before its last message and resumes at it" do
    rows = [row(0, 0, "a"), row(0, 1, "bc"), row(7, 0, "d"), row(7, 1, "e")]
    rows = Enum.with_index(rows, fn row, i -> Map.put(row, :key, <<i>>) end)

    page =
      %{
        rows: rows,
        cursor: <<4>>,
        cursor_exclusive: true,
        stopped: :deadline,
        hard_max_bytes: 65_536
      }
      |> Messages.end_at_known_boundary()

    assert %{next_message_offset: 7, cursor: <<2>>, cursor_exclusive: false} = page
    assert [%{interpretation: {:ok, "abc"}}, _] = Messages.annotate(page, &{:ok, &1}).rows

    single = %{rows: Enum.take(rows, 2), cursor: <<2>>, cursor_exclusive: true}
    assert %{next_message_offset: nil, cursor: <<2>>} = Messages.end_at_known_boundary(single)
  end

  defp row(offset, part, value) do
    %{
      value: value,
      decoded: {:ok, %{offset: offset, part: part}},
      truncated: false,
      oversized_key: false
    }
  end

  defp page(rows, tail) do
    %{
      rows: rows,
      next_message_offset: tail,
      hard_max_bytes: 65_536
    }
  end
end
