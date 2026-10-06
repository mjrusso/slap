defmodule Slap.Yjs.DocServer.Sync do
  @moduledoc false
  # The callers of Slap.Yjs.DocServer.sync/2. Each waits for the tail of the
  # document's stream as of its call, which a probe reads, then for the
  # server to have read up to it with nothing buffered or running. A caller
  # that gives up (its call timed out) or exits is removed: `from` is its
  # request, `monitor` the server's monitor of it.

  @type caller :: %{from: {pid(), reference()}, monitor: reference()}
  @type t :: %__MODULE__{
          waiting: [{caller(), non_neg_integer() | nil}],
          probing: boolean()
        }

  defstruct waiting: [], probing: false

  @spec add(t(), caller()) :: t()
  def add(s, caller), do: %{s | waiting: [{caller, nil} | s.waiting]}

  @spec remove(t(), reference()) :: {[caller()], t()}
  def remove(s, ref) do
    {removed, waiting} =
      Enum.split_with(s.waiting, fn {caller, _tail} ->
        ref in [elem(caller.from, 1), caller.monitor]
      end)

    {Enum.map(removed, &elem(&1, 0)), %{s | waiting: waiting}}
  end

  @doc """
  With nothing buffered or running: the callers whose tail has been read
  (to answer), and whether to start a probe for those without a tail.
  """
  @spec settled(t(), non_neg_integer()) :: {[caller()], :probe | :none, t()}
  def settled(s, read_offset) do
    {done, waiting} = Enum.split_with(s.waiting, fn {_, tail} -> tail && read_offset >= tail end)
    s = %{s | waiting: waiting}

    case not s.probing and Enum.any?(waiting, &match?({_, nil}, &1)) do
      true -> {Enum.map(done, &elem(&1, 0)), :probe, %{s | probing: true}}
      false -> {Enum.map(done, &elem(&1, 0)), :none, s}
    end
  end

  @spec probed(t(), {:ok, non_neg_integer()} | {:error, term()}) :: {[caller()], t()}
  def probed(s, {:ok, tail}) do
    waiting = Enum.map(s.waiting, fn {caller, t} -> {caller, t || tail} end)
    {[], %{s | waiting: waiting, probing: false}}
  end

  def probed(s, {:error, _}) do
    {failed, waiting} = Enum.split_with(s.waiting, &match?({_, nil}, &1))
    {Enum.map(failed, &elem(&1, 0)), %{s | waiting: waiting, probing: false}}
  end
end
