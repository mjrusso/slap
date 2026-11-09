defmodule Slap.Inspect.Capture do
  @moduledoc false

  alias Slap.Inspect.Remote

  # Keep log-content limits local so servers do not need slap_yjs.
  @log_limits %{
    defaults: %{input_bytes: 8 * 1024 * 1024, entries: 50_000},
    maximums: %{input_bytes: 32 * 1024 * 1024, entries: 200_000}
  }

  def prepare(connection, request, deadline) do
    with {:ok, advertised} <- advertised(connection, request.operation, deadline),
         {:ok, limits} <- limits(advertised, request.opts),
         {:ok, capture} <- capture(connection, request, deadline) do
      if capture.encoded_bytes > limits.input_bytes do
        {:error, {:too_large, capture.encoded_bytes, limits.input_bytes}}
      else
        {:ok, Map.put(capture, :limits, limits)}
      end
    end
  end

  defp advertised(_connection, :log_content, _deadline), do: {:ok, @log_limits}

  defp advertised(connection, :yjs_content, deadline) do
    with {:ok, budget} <- Remote.remaining(deadline),
         do: Remote.call(connection.entry, Slap.Yjs.Introspect, :limits, [], min(budget, 5000))
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
