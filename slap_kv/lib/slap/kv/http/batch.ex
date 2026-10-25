defmodule Slap.KV.HTTP.Batch do
  @moduledoc false

  def decode(body, max_value) do
    with {:ok, %{"ops" => ops} = request} <- JSON.decode(body),
         true <- allowed?(request, ["ops", "deadline"]),
         {:ok, deadline} <- deadline(request),
         {:ok, ops} <- operations(ops, max_value) do
      {:ok, ops, deadline}
    else
      {:error, {:bad_request, _}} = error -> error
      _ -> bad_request(:invalid_batch)
    end
  end

  defp deadline(request) do
    case Map.fetch(request, "deadline") do
      :error -> {:ok, []}
      {:ok, ms} when is_integer(ms) -> {:ok, [deadline: ms]}
      _ -> bad_request(:invalid_deadline)
    end
  end

  defp operations(ops, max) when is_list(ops) do
    Enum.reduce_while(ops, {:ok, []}, fn op, {:ok, acc} ->
      case operation(op, max) do
        {:ok, op} -> {:cont, {:ok, [op | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      error -> error
    end
  end

  defp operations(_ops, _max), do: bad_request(:invalid_batch)

  defp operation(%{"op" => "put", "key" => key, "value" => value} = op, max) do
    with true <- allowed?(op, ["op", "key", "value", "if_version", "if_absent"]),
         {:ok, key} <- decode64(key),
         {:ok, value} <- decode64(value),
         :ok <- value_size(value, max),
         {:ok, condition} <- condition(op, :put) do
      {:ok, List.to_tuple([:put, key, value] ++ condition)}
    else
      {:error, _} = error -> error
      _ -> bad_request(:invalid_operation)
    end
  end

  defp operation(%{"op" => name, "key" => key} = op, _max) when name in ["delete", "check"] do
    tag = tag(name)

    with true <- allowed?(op, ["op", "key", "if_version", "if_absent"]),
         {:ok, key} <- decode64(key),
         {:ok, condition} <- condition(op, tag) do
      {:ok, List.to_tuple([tag, key] ++ condition)}
    else
      {:error, _} = error -> error
      _ -> bad_request(:invalid_operation)
    end
  end

  defp operation(_op, _max), do: bad_request(:invalid_operation)

  defp tag("delete"), do: :delete
  defp tag("check"), do: :check

  defp condition(op, tag) do
    case {Map.fetch(op, "if_version"), Map.fetch(op, "if_absent")} do
      {:error, :error} when tag != :check -> {:ok, []}
      {{:ok, v}, :error} when is_integer(v) and v >= 0 -> {:ok, [v]}
      {:error, {:ok, true}} when tag in [:put, :check] -> {:ok, [:absent]}
      _ -> bad_request(:invalid_version)
    end
  end

  defp decode64(value) when is_binary(value) do
    with {:ok, decoded} <- Base.url_decode64(value, padding: false),
         ^value <- Base.url_encode64(decoded, padding: false) do
      {:ok, decoded}
    else
      _ -> bad_request(:invalid_base64)
    end
  end

  defp decode64(_value), do: bad_request(:invalid_base64)
  defp allowed?(map, keys), do: Enum.all?(Map.keys(map), &(&1 in keys))
  defp value_size(value, max) when byte_size(value) <= max, do: :ok
  defp value_size(_value, _max), do: bad_request(:value_too_large)
  defp bad_request(reason), do: {:error, {:bad_request, reason}}
end
