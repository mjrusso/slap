defmodule Slap.Inspect.Messages do
  @moduledoc false

  def assemble(page) do
    with {:ok, fragments} <- fragments(page) do
      {:ok,
       Enum.map(fragments, fn {fragment, value} ->
         message(fragment, value, fragment.status == :complete and is_nil(fragment.before))
       end)}
    end
  end

  def accumulate(page, pending) do
    with {:ok, fragments} <- fragments(page),
         {:ok, messages, pending} <-
           Enum.reduce_while(fragments, {:ok, [], pending}, &accumulate_fragment/2) do
      {:ok, Enum.reverse(messages), pending}
    end
  end

  defp accumulate_fragment({fragment, value}, {:ok, messages, pending}) do
    case append(fragment, value, pending) do
      {:ok, values} -> finish(fragment, values, messages)
      error -> {:halt, error}
    end
  end

  defp fragments(%{fragment_contract: 1, fragments: fragments, rows: rows}) do
    counts = Enum.map(fragments, & &1.row_count)

    if Enum.all?(counts, &(is_integer(&1) and &1 > 0)) and Enum.sum(counts) == length(rows) do
      {fragments, []} =
        Enum.map_reduce(fragments, rows, fn fragment, rows ->
          {covered, rest} = Enum.split(rows, fragment.row_count)
          value = covered |> Enum.map(& &1.value) |> IO.iodata_to_binary()
          {{fragment, value}, rest}
        end)

      {:ok, fragments}
    else
      {:error, :invalid_message_fragments}
    end
  end

  defp fragments(_page), do: {:error, :unsupported_message_fragments}

  defp append(%{before: nil}, value, nil), do: {:ok, [value]}

  defp append(%{before: token}, value, %{token: token, values: values}),
    do: {:ok, [value | values]}

  defp append(_fragment, _value, _pending), do: {:error, :incomplete_message_parts}

  defp finish(%{status: :more, after: token}, values, messages),
    do: {:cont, {:ok, messages, %{token: token, values: values}}}

  defp finish(%{status: :complete} = fragment, values, messages) do
    value = values |> Enum.reverse() |> IO.iodata_to_binary()
    {:cont, {:ok, [message(fragment, value, true) | messages], nil}}
  end

  defp finish(_fragment, _values, _messages),
    do: {:halt, {:error, :incomplete_message_parts}}

  defp message(fragment, value, complete) do
    %{
      offset: fragment.offset,
      value: value,
      complete: complete,
      next_offset: fragment.next_offset,
      diagnostic: if(complete, do: nil, else: :incomplete_message_parts)
    }
  end
end
