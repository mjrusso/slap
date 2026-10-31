defmodule Slap.Inspect.Collection do
  @moduledoc false

  alias Slap.Inspect.{Messages, Records, Request}

  @doc "Reads the messages of `stream` (its `path` and `sid`) in `from..until`."
  def read(connection, request, stream, from, until, limits, deadline) do
    opts =
      request.opts
      |> Keyword.drop(Request.page_options())
      |> Keyword.merge(path: stream.path, expected_sid: stream.sid, from: from, until: until)

    state = %{messages: [], pending: nil, bytes: 0, entries: 0}

    with {:ok, request} <- Request.derive(request, :logical, opts),
         do: pages(connection, request, limits, deadline, state)
  end

  defp pages(connection, request, limits, deadline, state) do
    with {:ok, page} <- Records.page(connection, request, deadline),
         {:ok, state} <- consume(page, state, limits) do
      next_page(connection, request, page, limits, deadline, state)
    end
  end

  defp consume(%{fragment_contract: 1} = page, state, limits) do
    bytes = state.bytes + Enum.reduce(page.fragments, 0, &(&1.encoded_bytes + &2))

    if bytes > limits.input_bytes do
      {:error, {:too_large, bytes, limits.input_bytes}}
    else
      assemble(page, %{state | bytes: bytes}, limits)
    end
  end

  defp consume(_page, _state, _limits), do: {:error, :unsupported_message_fragments}

  defp assemble(page, state, limits) do
    with {:ok, complete, pending} <- Messages.accumulate(page, state.pending) do
      count = state.entries + length(complete)

      if count > limits.entries do
        {:error, {:too_large, count, limits.entries}}
      else
        {:ok,
         %{
           state
           | pending: pending,
             entries: count,
             messages: Enum.reverse(complete, state.messages)
         }}
      end
    end
  end

  defp next_page(
         _connection,
         _request,
         %{cursor: nil},
         _limits,
         _deadline,
         %{pending: nil} = state
       ) do
    {:ok,
     %{
       messages: Enum.reverse(state.messages),
       encoded_bytes: state.bytes
     }}
  end

  defp next_page(_connection, _request, %{cursor: nil}, _limits, _deadline, _state),
    do: {:error, :incomplete_message_parts}

  defp next_page(connection, request, page, limits, deadline, state) do
    if page.cursor == request.opts[:cursor] do
      {:error, :incomplete}
    else
      opts =
        Keyword.merge(request.opts, cursor: page.cursor, cursor_exclusive: page.cursor_exclusive)

      pages(connection, %{request | opts: opts}, limits, deadline, state)
    end
  end

  def retained_from(messages, until) do
    messages
    |> Enum.reverse()
    |> Enum.reduce_while(until, fn message, next ->
      if message.next_offset == next,
        do: {:cont, message.offset},
        else: {:halt, next}
    end)
  end
end
