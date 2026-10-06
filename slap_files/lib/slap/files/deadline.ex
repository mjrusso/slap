defmodule Slap.Files.Deadline do
  @moduledoc false
  # Slap.KV writes that an intent covers are not applied once it is due
  # (see Slap.Files.Sweeper).

  alias Slap.Files.Config

  # The Slap.KV options for a write that must not be applied from `due_ms`
  # (by Config.now/0) on, or at any time if it is nil.
  @spec opts(integer() | nil, Config.t()) :: keyword()
  def opts(due_ms, config)
  def opts(nil, _config), do: []
  def opts(due_ms, config), do: [deadline: Config.system_time(due_ms, config)]

  # A write that took too long is a timeout to Slap.Files' callers; this
  # one is known not to be applied, which they are not promised.
  @spec result(result) :: result when result: term()
  def result({:error, :deadline_exceeded}), do: {:error, :timeout}
  def result(result), do: result
end
