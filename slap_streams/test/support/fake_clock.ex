defmodule Slap.Streams.Test.FakeClock do
  @moduledoc false
  # A clock for Slap.Streams.Clock that only moves when told to.

  @key {__MODULE__, :now}

  @doc "Installs the clock at `start_ms`; `uninstall/0` restores the real one."
  def install(start_ms) do
    ref = :atomics.new(1, signed: true)
    :atomics.put(ref, 1, start_ms)
    :persistent_term.put(@key, ref)
    Application.put_env(:slap_streams, :clock, {__MODULE__, :now_ms, []})
  end

  def uninstall, do: Application.delete_env(:slap_streams, :clock)

  def now_ms, do: :atomics.get(:persistent_term.get(@key), 1)

  def advance(ms), do: :atomics.add_get(:persistent_term.get(@key), 1, ms)
end
