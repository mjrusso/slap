defmodule Slap.Yjs.Owners do
  @moduledoc false
  # Who set each awareness state a document server knows: one of its
  # subscribers (a pid), or another server of the document ({:server, pid}).
  # A state set by the server itself, or with no origin, has no owner.
  #
  # A client id has at most one owner, the last that set it: a client that
  # moves to another server is that server's from then on, and the old
  # server leaving does not remove it.

  @type owner :: pid() | {:server, pid()}
  @type t :: %{non_neg_integer() => owner()}

  @spec changed(t(), owner() | nil, [non_neg_integer()], [non_neg_integer()]) :: t()
  def changed(owners, owner, set, removed) do
    owners = Map.drop(owners, removed)

    case owner do
      nil -> Map.drop(owners, set)
      owner -> Enum.reduce(set, owners, &Map.put(&2, &1, owner))
    end
  end

  @spec forget(t(), owner()) :: {[non_neg_integer()], t()}
  def forget(owners, owner) do
    {gone, kept} = Map.split_with(owners, fn {_id, o} -> o == owner end)
    {Map.keys(gone), kept}
  end

  @doc "The client ids in `ids` not learned from another server."
  @spec local(t(), [non_neg_integer()]) :: [non_neg_integer()]
  def local(owners, ids), do: Enum.reject(ids, &match?({:server, _}, owners[&1]))
end
