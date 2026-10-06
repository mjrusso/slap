defmodule Slap.SlateDB.Snapshot do
  @moduledoc """
  A consistent, read-only view of the database at the time it was taken.

  Create one with `Slap.SlateDB.snapshot/1`. Writes made after the snapshot are
  not visible through it.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Iterator, Read}

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @doc "Reads `key` as of the snapshot. Takes the options of `Slap.SlateDB.get/3`."
  @spec get(t(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, SlateDB.Error.t()}
  # Typed entry points to the shared read path (`Slap.SlateDB.Read` and
  # `Slap.SlateDB.Iterator`). Each handle module has the same ones, so the
  # duplication checker is told to skip them.
  # ex_dna:disable-for-next-line
  def get(%__MODULE__{} = snapshot, key, opts \\ []), do: Read.get(snapshot, key, opts)

  @doc """
  Reads `key` with its metadata as of the snapshot, like
  `Slap.SlateDB.get_key_value/3`.
  """
  @spec get_key_value(t(), binary(), keyword()) ::
          {:ok, SlateDB.key_value() | nil} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def get_key_value(%__MODULE__{} = snapshot, key, opts \\ []),
    do: Read.get_key_value(snapshot, key, opts)

  @doc "Scans rows as of the snapshot. Takes the same options as `Slap.SlateDB.scan/2`."
  @spec scan(t(), keyword()) :: Enumerable.t()
  # ex_dna:disable-for-next-line
  def scan(%__MODULE__{} = snapshot, opts \\ []), do: Iterator.stream(snapshot, opts)

  @doc "Opens a `Slap.SlateDB.Iterator` as of the snapshot. Takes the options of `Slap.SlateDB.iterator/2`."
  @spec iterator(t(), keyword()) :: {:ok, Iterator.t()} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def iterator(%__MODULE__{} = snapshot, opts \\ []), do: Iterator.open(snapshot, opts)
end
