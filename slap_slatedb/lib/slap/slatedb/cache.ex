defmodule Slap.SlateDB.Cache do
  @moduledoc """
  An in-memory block and metadata cache that several databases can share.

      cache = Slap.SlateDB.Cache.new(512 * 1024 * 1024)
      {:ok, a} = Slap.SlateDB.open("a", store: store, cache: cache)
      {:ok, b} = Slap.SlateDB.open("b", store: store, cache: cache)

  The databases compete for one memory budget instead of each having its own.
  SlateDB keeps each database's entries apart, so they cannot see each other's
  blocks. Without a `:cache` option, each database gets its own default cache.
  """

  alias Slap.SlateDB

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @doc "Creates a cache that holds up to `capacity_bytes` of blocks and metadata."
  @spec new(pos_integer()) :: t()
  def new(capacity_bytes) when is_integer(capacity_bytes) and capacity_bytes > 0 do
    %__MODULE__{resource: SlateDB.Native.cache_new(capacity_bytes)}
  end
end
