defmodule Slap.Yjs.Owners do
  @moduledoc false
  # Claims authorize local submissions; cleanup follows the last writer, which
  # may be a peer without a local claim. A claim transfers existing cleanup
  # ownership even when Yjs ignores the replacement's equal-clock state.

  defstruct claims: %{}, revoked: %{}, cleanup: %{}

  @type owner :: pid() | {:server, pid()}
  @type t :: %__MODULE__{
          claims: %{non_neg_integer() => {pid(), term()}},
          revoked: %{pid() => MapSet.t(non_neg_integer())},
          cleanup: %{non_neg_integer() => owner()}
        }

  @spec claim(t(), non_neg_integer(), pid(), term()) ::
          {:ok, t()} | {:error, :not_owner | :claimed}
  def claim(owners, id, pid, principal) do
    case {MapSet.member?(Map.get(owners.revoked, pid, MapSet.new()), id), owners.claims[id]} do
      {true, _} -> {:error, :not_owner}
      {false, nil} -> {:ok, put_claim(owners, id, pid, principal)}
      {false, {^pid, ^principal}} -> {:ok, owners}
      {false, {previous, ^principal}} -> {:ok, takeover(owners, id, previous, pid, principal)}
      {false, _} -> {:error, :claimed}
    end
  end

  @spec owns?(t(), non_neg_integer(), pid()) :: boolean()
  def owns?(owners, id, pid), do: match?({^pid, _}, owners.claims[id])

  @spec changed(t(), owner() | nil, [non_neg_integer()], [non_neg_integer()]) :: t()
  def changed(owners, owner, set, removed) do
    cleanup = Map.drop(owners.cleanup, removed)

    cleanup =
      case owner do
        nil -> Map.drop(cleanup, set)
        owner -> Enum.reduce(set, cleanup, &Map.put(&2, &1, owner))
      end

    %{owners | cleanup: cleanup}
  end

  @spec forget(t(), owner()) :: {[non_neg_integer()], t()}
  def forget(owners, owner) do
    {gone, kept} = Map.split_with(owners.cleanup, fn {_id, o} -> o == owner end)

    owners = %{
      owners
      | cleanup: kept,
        claims: Map.reject(owners.claims, fn {_id, {pid, _principal}} -> pid == owner end),
        revoked: Map.delete(owners.revoked, owner)
    }

    {Map.keys(gone), owners}
  end

  @doc "The client ids in `ids` not learned from another server."
  @spec local(t(), [non_neg_integer()]) :: [non_neg_integer()]
  def local(owners, ids), do: Enum.reject(ids, &match?({:server, _}, owners.cleanup[&1]))

  defp put_claim(owners, id, pid, principal) do
    %{
      owners
      | claims: Map.put(owners.claims, id, {pid, principal}),
        cleanup: Map.replace(owners.cleanup, id, pid)
    }
  end

  defp takeover(owners, id, previous, pid, principal) do
    revoked = Map.update(owners.revoked, previous, MapSet.new([id]), &MapSet.put(&1, id))
    put_claim(%{owners | revoked: revoked}, id, pid, principal)
  end
end
