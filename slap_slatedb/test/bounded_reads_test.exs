defmodule Slap.SlateDB.BoundedReadsTest do
  use ExUnit.Case, async: true

  alias Slap.SlateDB
  alias Slap.SlateDB.Iterator

  setup do
    {:ok, db} = SlateDB.open("bounded", store: :memory)
    on_exit(fn -> SlateDB.close(db) end)
    %{db: db}
  end

  test "bounded gets copy only a preview and preserve the whole size", %{db: db} do
    bytes = :binary.copy(<<0, 255>>, 600_000)
    assert {:ok, _} = SlateDB.put(db, "large", bytes, await_durable: true)

    assert {:ok, %{value: prefix, value_size: 1_200_000, offset: 0}} =
             SlateDB.get_range(db, "large", max_value_bytes: 70_000)

    assert prefix == binary_part(bytes, 0, 70_000)
    assert :binary.referenced_byte_size(prefix) == 70_000

    assert {:ok, %{value: <<255, 0, 255>>, value_size: 1_200_000, offset: 1}} =
             SlateDB.get_range(db, "large", value_range: {1, 3}, max_value_bytes: 10)

    assert {:ok, %{value: <<>>, value_size: 1_200_000, offset: 2_000_000}} =
             SlateDB.get_range(db, "large", value_range: {2_000_000, 1}, max_value_bytes: 10)

    assert {:ok, %{value: <<0, 255>>, value_size: 1_200_000}} =
             SlateDB.get_range(db, "large", value_range: {0, 10}, max_value_bytes: 2)

    assert {:ok, nil} = SlateDB.get_range(db, "absent", max_value_bytes: 1)
    assert {:ok, ^bytes} = SlateDB.get(db, "large")
  end

  test "preview iterators return versions and full sizes with cut keys and values", %{db: db} do
    key = :binary.copy("k", 1_100_000)
    value = :binary.copy("v", 1_200_000)
    assert {:ok, seq} = SlateDB.put(db, key, value, await_durable: true)

    assert [{"kkkk", "vvv", ^seq, 1_100_000, 1_200_000}] =
             preview(db, max_key_bytes: 4, max_value_bytes: 3)

    assert [{"k", ^value, ^seq, 1_100_000, 1_200_000}] = preview(db, max_key_bytes: 1)
    assert [{^key, "", ^seq, 1_100_000, 1_200_000}] = preview(db, max_value_bytes: 0)
  end

  test "limits and ranges reject invalid arguments", %{db: db} do
    for opts <- [
          [max_value_bytes: nil],
          [max_value_bytes: -1],
          [max_value_bytes: 1, value_range: {-1, 1}],
          [max_value_bytes: 1, value_range: {0, -1}],
          [value_range: {0, 1}]
        ] do
      assert_raise ArgumentError, fn -> SlateDB.get_range(db, "key", opts) end
    end

    for opts <- [[max_value_bytes: 1], [value_range: {0, 1}]] do
      assert_raise ArgumentError, fn -> SlateDB.get(db, "key", opts) end
      assert_raise ArgumentError, fn -> SlateDB.get_key_value(db, "key", opts) end
    end

    for opts <- [[max_key_bytes: 1], [max_value_bytes: 1]] do
      assert_raise ArgumentError, fn -> db |> SlateDB.scan(opts) |> Enum.to_list() end
      assert_raise ArgumentError, fn -> SlateDB.iterator(db, opts) end
    end

    for opts <- [
          [],
          [max_key_bytes: nil, max_value_bytes: nil],
          [max_key_bytes: -1],
          [max_value_bytes: 1, with_versions: true]
        ] do
      assert_raise ArgumentError, fn -> SlateDB.preview_iterator(db, opts) end
    end
  end

  defp preview(db, opts) do
    {:ok, iter} = SlateDB.preview_iterator(db, opts)
    {:ok, rows} = Iterator.next_batch(iter)
    assert {:ok, []} = Iterator.next_batch(iter)
    rows
  end
end
