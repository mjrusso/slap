defmodule Slap.SlateDB.MergeOperator do
  @moduledoc """
  The built-in merge operators, and helpers for their operand formats.

  A merge operator lets you update a value without reading it first:
  `Slap.SlateDB.merge/4` writes an operand, and SlateDB combines the operands with
  the key's value when the key is read or compacted. Choose one when opening
  the database:

      {:ok, db} = Slap.SlateDB.open("counters", store: store, merge_operator: :u64_add)
      {:ok, _} = Slap.SlateDB.increment(db, "page:home:views")
      {:ok, _} = Slap.SlateDB.merge(db, "page:home:views", Slap.SlateDB.MergeOperator.encode_u64(10))
      {:ok, bin} = Slap.SlateDB.get(db, "page:home:views")
      11 = Slap.SlateDB.MergeOperator.decode_u64(bin)

  | Operator | Operand and value | Result |
  | --- | --- | --- |
  | `:u64_add` | unsigned 64-bit little-endian integer (8 bytes) | sum, wrapping at 2^64 |
  | `:i64_add` | signed 64-bit little-endian integer (8 bytes) | sum, wrapping |
  | `:u64_max` | unsigned 64-bit little-endian integer (8 bytes) | largest |
  | `:u64_min` | unsigned 64-bit little-endian integer (8 bytes) | smallest |
  | `:append` | any binary | the value followed by each operand, oldest first |

  A `put/4` sets the base value that later operands merge into, and a
  `delete/3` clears it.

  ## Things to know

    * The operators run in Rust. SlateDB calls a merge operator from its own
      threads on reads, flushes and compactions, where calling back into
      Elixir would be slow and could deadlock. So custom Elixir operators are
      not supported.
    * Operands are checked when they are written: a numeric operand that is
      not 8 bytes is rejected with an `:invalid` error.
    * A `put/4` value for a key that numeric operands merge into must also be
      8 bytes. SlateDB cannot check that at write time. If it is not, reads
      of that key fail, and so does any compaction that includes it.
    * Every process that opens the database (writers, `Slap.SlateDB.Reader`s, a
      separate compactor) needs the same operator. Without one, reading a
      key that has operands fails with an `:invalid` error.
  """

  @doc "Encodes an unsigned integer as a `:u64_add`, `:u64_max` or `:u64_min` operand."
  @spec encode_u64(non_neg_integer()) :: binary()
  def encode_u64(n) when is_integer(n) and n >= 0 and n < 18_446_744_073_709_551_616,
    do: <<n::unsigned-little-64>>

  @doc "Decodes a value written by a `:u64_*` operator. `nil` stays `nil`."
  @spec decode_u64(binary() | nil) :: non_neg_integer() | nil
  def decode_u64(nil), do: nil
  def decode_u64(<<n::unsigned-little-64>>), do: n

  @doc "Encodes a signed integer as an `:i64_add` operand."
  @spec encode_i64(integer()) :: binary()
  def encode_i64(n)
      when is_integer(n) and n >= -9_223_372_036_854_775_808 and
             n <= 9_223_372_036_854_775_807,
      do: <<n::signed-little-64>>

  @doc "Decodes a value written by `:i64_add`. `nil` stays `nil`."
  @spec decode_i64(binary() | nil) :: integer() | nil
  def decode_i64(nil), do: nil
  def decode_i64(<<n::signed-little-64>>), do: n
end
