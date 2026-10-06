defmodule Slap.SlateDB.CompactionFilter do
  @moduledoc """
  Deletes every key under a set of prefixes, as the database is compacted.

  This is a cheap way to delete a large range of keys, such as everything
  stored for a deleted stream: instead of writing a tombstone for every key,
  add the prefix to the filter, and compactions delete the keys as they
  rewrite them.

      filter = Slap.SlateDB.CompactionFilter.new()
      {:ok, db} = Slap.SlateDB.open("streams", store: store, compaction_filter: filter)
      # ... later, when stream 42 is deleted:
      :ok = Slap.SlateDB.CompactionFilter.add(filter, ["stream/42/"])

  ## Things to know

    * **Keys stay readable until they are compacted.** The filter only
      changes data that a compaction rewrites. Until then, reads still see
      the keys, so also stop reading them (for example, check your own
      "deleted" marker first).
    * **Data that is already compacted waits for its sorted run to be
      compacted again.** New writes are compacted soon, but a key that sits
      in a sorted run is only deleted when a compaction rewrites that run,
      which can take a long time. Call `Slap.SlateDB.Admin.compact/2` after adding
      prefixes to rewrite all sorted runs.
    * **Deleting is permanent.** Once a compaction has deleted a key,
      removing its prefix from the filter does not bring it back.
    * **Matching entries become tombstones.** A dropped entry could uncover
      an older version of the key in a part of the tree that the compaction
      did not include. A tombstone hides older versions, and SlateDB removes
      it when it reaches the bottom of the tree.
    * **Snapshots and transactions may see the change.** A compaction does not
      keep deleted keys for open snapshots. This is how SlateDB compaction
      filters work in general.
    * **Each compaction takes the set as it is when it starts.**
    * **Only this process's compactor uses the filter.** A compactor run
      elsewhere needs its own.
    * The filter keeps its prefixes in memory. Keep the set small (for example
      remove a prefix after every key under it is gone), because every change
      rebuilds it.
  """

  alias Slap.SlateDB.Native

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @doc "Creates a filter with an initial set of prefixes."
  @spec new([binary()]) :: t()
  def new(prefixes \\ []) when is_list(prefixes) do
    %__MODULE__{resource: Native.prefix_filter_new(check(prefixes))}
  end

  @doc "Adds prefixes. Later compactions delete keys under them."
  @spec add(t(), [binary()]) :: :ok
  def add(%__MODULE__{resource: filter}, prefixes) when is_list(prefixes) do
    Native.prefix_filter_update(filter, check(prefixes), [])
  end

  @doc "Removes prefixes. Keys already deleted stay deleted."
  @spec remove(t(), [binary()]) :: :ok
  def remove(%__MODULE__{resource: filter}, prefixes) when is_list(prefixes) do
    Native.prefix_filter_update(filter, [], check(prefixes))
  end

  @doc "Returns the prefixes, sorted."
  @spec prefixes(t()) :: [binary()]
  def prefixes(%__MODULE__{resource: filter}) do
    {prefixes, _tombstoned} = Native.prefix_filter_info(filter)
    prefixes
  end

  @doc "Returns how many entries compactions have turned into tombstones so far."
  @spec tombstoned(t()) :: non_neg_integer()
  def tombstoned(%__MODULE__{resource: filter}) do
    {_prefixes, tombstoned} = Native.prefix_filter_info(filter)
    tombstoned
  end

  defp check(prefixes) do
    Enum.each(prefixes, fn
      prefix when is_binary(prefix) and byte_size(prefix) > 0 ->
        :ok

      other ->
        raise ArgumentError, "prefixes must be non-empty binaries, got: #{inspect(other)}"
    end)

    prefixes
  end
end
