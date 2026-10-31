defmodule Slap.Yjs.Updates do
  @moduledoc false

  # yrs can leave updates pending after their predecessors have integrated.
  # `updates` can be lazy: an `{:error, reason}` element stops at that point.
  def apply(doc, updates, origin, deadline \\ :infinity) do
    Yex.Doc.transaction(doc, origin, fn ->
      with :ok <- apply_all(doc, updates, deadline),
           {:ok, pending} <- Yex.Doc.prune_pending(doc) do
        apply_all(doc, List.wrap(pending), deadline)
      end
    end)
  end

  defp apply_all(doc, updates, deadline) do
    Enum.reduce_while(updates, :ok, fn update, :ok ->
      case apply_one(doc, update, deadline) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp apply_one(_doc, {:error, _reason} = error, _deadline), do: error

  defp apply_one(doc, update, deadline) do
    if deadline != :infinity and System.monotonic_time(:millisecond) >= deadline,
      do: {:error, :deadline},
      else: Yex.apply_update(doc, update)
  end
end
