defmodule Slap.Streams.Clock do
  @moduledoc false

  @doc "The current time in Unix milliseconds."
  @spec now_ms() :: integer()
  def now_ms do
    case Application.get_env(:slap_streams, :clock) do
      nil -> System.system_time(:millisecond)
      {m, f, a} -> apply(m, f, a)
    end
  end
end
