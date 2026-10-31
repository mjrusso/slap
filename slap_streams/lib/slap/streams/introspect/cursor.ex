defmodule Slap.Streams.Introspect.Cursor do
  @moduledoc false

  alias Slap.Cluster.Inspection.Read
  alias Slap.Streams.Store.Keys

  # The longest message-key suffix an inspection scan can return.
  @max_suffix Read.max_key_bytes() - byte_size(Keys.msg_prefix(0))

  def at(offset), do: encode(offset, 0, <<>>, false, nil)

  def position(nil), do: {:ok, 0}

  def position(cursor) do
    with {:ok, suffix, _exclusive, _state} <- decode(cursor),
         do: {:ok, offset(suffix)}
  end

  def options(opts, sid),
    do: options(Keyword.delete(opts, :fragment_state), sid, opts[:cursor])

  defp options(opts, _sid, nil), do: {:ok, opts}

  defp options(opts, sid, cursor) do
    case message_suffix(cursor, sid) do
      {:ok, _suffix} ->
        if opts[:logical_cursor], do: {:error, :invalid_logical_cursor}, else: {:ok, opts}

      :error ->
        decoded_options(opts, sid, cursor)
    end
  end

  defp decoded_options(opts, sid, cursor) do
    with {:ok, suffix, exclusive, state} <- decode(cursor),
         true <- is_nil(state) or state.sid == sid do
      opts = Keyword.drop(opts, [:cursor, :cursor_exclusive])
      key = Keys.msg_prefix(sid) <> suffix

      if key >= Keys.msg(sid, Keyword.get(opts, :from, 0), 0),
        do:
          {:ok,
           Keyword.merge(opts,
             cursor: key,
             cursor_exclusive: exclusive,
             fragment_state: state
           )},
        else: {:ok, opts}
    else
      false -> {:error, :invalid_logical_cursor}
      error -> error
    end
  end

  def page(%{cursor: nil} = page, _state), do: {:ok, page}

  def page(%{sid: sid, cursor: cursor} = page, state),
    do: page(page, message_suffix(cursor, sid), state)

  defp page(page, {:ok, <<offset::64, part::16, suffix::binary>>}, state)
       when suffix in [<<>>, <<0>>] do
    {:ok,
     %{
       page
       | cursor: encode(offset, part, suffix, page.cursor_exclusive, state),
         cursor_exclusive: false
     }}
  end

  defp page(page, {:ok, suffix}, _state) when byte_size(suffix) <= @max_suffix do
    exclusive = if page.cursor_exclusive, do: 1, else: 0
    {:ok, %{page | cursor: <<3, exclusive, suffix::binary>>, cursor_exclusive: false}}
  end

  # A cursor past the stream's messages ends the page.
  defp page(page, :error, _state) do
    if page.cursor > Keys.msg_prefix(page.sid),
      do: {:ok, %{page | cursor: nil, cursor_exclusive: false}},
      else: {:error, :invalid_logical_cursor}
  end

  defp page(_page, {:ok, _suffix}, _state), do: {:error, :invalid_logical_cursor}

  defp message_suffix(key, sid) do
    prefix = Keys.msg_prefix(sid)
    size = byte_size(prefix)

    case key do
      <<^prefix::binary-size(^size), suffix::binary>> -> {:ok, suffix}
      _other -> :error
    end
  end

  defp offset(suffix) do
    <<offset::64, _rest::binary>> = suffix <> <<0::64>>

    # Partial keys can sort before part zero; routing must preserve that lower bound.
    if suffix < <<offset::64, 0::16>>, do: max(offset - 1, 0), else: offset
  end

  defp encode(offset, part, suffix, exclusive, state) do
    next = if suffix == <<0>>, do: 1, else: 0
    exclusive = if exclusive, do: 1, else: 0
    position = <<offset::64, part::16, next, exclusive>>

    case state do
      nil ->
        <<1, position::binary>>

      %{sid: sid, shard: shard, offset: at, next_part: next_part, bytes: bytes} ->
        <<2, position::binary, sid::64, shard::64, at::64, next_part::32, bytes::64>>
    end
  end

  defp decode(<<1, offset::64, part::16, next, exclusive>>)
       when next in [0, 1] and exclusive in [0, 1] do
    suffix = if next == 1, do: <<0>>, else: <<>>
    {:ok, <<offset::64, part::16, suffix::binary>>, exclusive == 1, nil}
  end

  defp decode(
         <<2, offset::64, part::16, next, exclusive, sid::64, shard::64, offset::64,
           next_part::32, bytes::64>>
       )
       when next in [0, 1] and exclusive in [0, 1] and next_part == part + 1 do
    suffix = if next == 1, do: <<0>>, else: <<>>
    state = %{sid: sid, shard: shard, offset: offset, next_part: next_part, bytes: bytes}
    {:ok, <<offset::64, part::16, suffix::binary>>, exclusive == 1, state}
  end

  defp decode(<<3, exclusive, suffix::binary>>)
       when exclusive in [0, 1] and byte_size(suffix) <= @max_suffix,
       do: {:ok, suffix, exclusive == 1, nil}

  defp decode(_cursor), do: {:error, :invalid_logical_cursor}
end
