defmodule Slap.Inspect.Capture do
  @moduledoc false

  alias Slap.Inspect.Remote

  def prepare(connection, request, deadline) do
    with {:ok, budget} <- Remote.remaining(deadline),
         {:ok, advertised} <-
           Remote.call(connection.entry, Slap.Yjs.Introspect, :limits, [], min(budget, 5000)),
         {:ok, limits} <- limits(advertised, request.opts),
         {:ok, capture} <- capture(connection, request, deadline) do
      if capture.encoded_bytes > limits.input_bytes do
        {:error, {:too_large, capture.encoded_bytes, limits.input_bytes}}
      else
        {:ok, Map.put(capture, :limits, limits)}
      end
    end
  end

  defp capture(connection, request, deadline) do
    capture = %{request | opts: Keyword.take(request.opts, [:base, :at_offset])}
    Remote.read(connection, capture, {Slap.SnapshotLog.Introspect, :capture}, deadline)
  end

  defp limits(advertised, opts) do
    requested = %{
      input_bytes: opts[:max_input_bytes],
      entries: opts[:max_entries],
      output_bytes: opts[:max_output_bytes]
    }

    requested = Map.reject(requested, fn {_key, value} -> is_nil(value) end)
    limits = Map.merge(advertised.defaults, requested)

    if Enum.all?(limits, fn {key, n} ->
         is_integer(n) and n > 0 and n <= advertised.maximums[key]
       end), do: {:ok, limits}, else: {:error, :invalid_reconstruction_limits}
  end
end
