defmodule Slap.Streams.HTTP.Cursor do
  @moduledoc false

  # The wire cursor counts 20-second intervals since 2024-10-09. A client
  # at or ahead of the current interval gets a later one, so a CDN cannot
  # collapse its wait into an earlier response.

  @epoch_ms DateTime.to_unix(~U[2024-10-09 00:00:00Z], :millisecond)
  @interval_ms 20_000
  # Like the official server, a random 1..3600 s ahead, in intervals.
  @max_jitter_intervals div(3600, 20)

  @doc "The cursor to return, given the client's `cursor` query parameter."
  @spec next(String.t() | nil) :: String.t()
  def next(client_cursor) do
    current = div(System.system_time(:millisecond) - @epoch_ms, @interval_ms)

    case client_cursor && Integer.parse(client_cursor) do
      {client, ""} when client >= current ->
        Integer.to_string(client + :rand.uniform(@max_jitter_intervals))

      _ ->
        Integer.to_string(current)
    end
  end
end
