defmodule Slap.Inspect.View.Activity do
  @moduledoc false
  import Breeze.Component, only: [assign: 2]

  @frames ~w(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
  @tick 100
  @delay 200

  def sync(%{assigns: %{operation: :idle}} = term), do: stop(term)
  def sync(%{assigns: %{id: id, activity: %{id: id}}} = term), do: term

  def sync(term) do
    term = stop(term)
    now = now()
    id = term.assigns.id
    assign(term, activity: %{id: id, since: now, now: now, stalled: false, timer: schedule(id)})
  end

  def tick(%{assigns: %{activity: %{id: id} = activity}} = term, id),
    do: assign(term, activity: %{activity | now: now(), timer: schedule(id)})

  def tick(term, _id), do: term

  def stalled(%{assigns: %{activity: %{} = activity}} = term),
    do: assign(term, activity: %{activity | stalled: true})

  def stalled(term), do: term

  def line(activity, status) do
    if shown?(activity), do: spinner(activity, status), else: status
  end

  defp spinner(activity, status) do
    elapsed = activity.now - activity.since
    frame = Enum.at(@frames, rem(div(elapsed, @tick), length(@frames)))
    seconds = :erlang.float_to_binary(elapsed / 1000, decimals: 1)
    "#{frame} #{seconds}s · #{status}"
  end

  def class(%{stalled: true}), do: "text-warning"
  def class(activity), do: if(shown?(activity), do: "text-accent", else: "")

  defp shown?(nil), do: false
  defp shown?(activity), do: activity.stalled or activity.now - activity.since >= @delay

  defp stop(%{assigns: %{activity: %{timer: timer}}} = term) do
    Process.cancel_timer(timer)
    assign(term, activity: nil)
  end

  defp stop(term), do: term

  defp schedule(id), do: Process.send_after(self(), {:activity_tick, id}, @tick)
  defp now, do: System.monotonic_time(:millisecond)
end
