defmodule Slap.Streams.Wait do
  @moduledoc "A pending stream wait. Use `ref/1` to match its wake message."

  @enforce_keys [:ref, :monitor, :opts]
  defstruct [:ref, :monitor, :opts]

  @opaque t :: %__MODULE__{ref: reference(), monitor: reference() | nil, opts: keyword()}

  @doc "Returns the reference included in this wait's wake message."
  @spec ref(t()) :: reference()
  def ref(%__MODULE__{ref: ref}), do: ref

  @doc false
  @spec new(reference(), reference() | nil, keyword()) :: t()
  def new(ref, monitor, opts), do: %__MODULE__{ref: ref, monitor: monitor, opts: opts}

  @doc false
  @spec cast(term()) :: {:ok, t()} | :error
  def cast(%__MODULE__{} = wait), do: {:ok, wait}
  def cast(_), do: :error

  @doc false
  @spec details(t()) :: {reference(), reference() | nil, keyword()}
  def details(%__MODULE__{ref: ref, monitor: monitor, opts: opts}), do: {ref, monitor, opts}
end
