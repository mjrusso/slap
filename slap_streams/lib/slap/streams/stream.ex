defmodule Slap.Streams.Stream do
  @moduledoc false

  alias Slap.Streams.Store.{CopyCreation, Meta, Producer, ReferenceCreation}

  @enforce_keys [
    :path,
    :status,
    :meta,
    :sid,
    :tail,
    :producers,
    :last_access,
    :expiry_key,
    :trim
  ]
  defstruct @enforce_keys

  @type status :: :absent | :active | :pending | :gone

  @type t :: %__MODULE__{
          path: binary(),
          status: status(),
          meta: Meta.t() | nil,
          sid: non_neg_integer() | nil,
          tail: non_neg_integer(),
          producers: %{binary() => Producer.t()},
          last_access: integer(),
          expiry_key: integer() | nil,
          trim: non_neg_integer()
        }

  defmodule View do
    @moduledoc false

    @enforce_keys [:status, :meta, :sid, :next_offset, :trim]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            status: Slap.Streams.Stream.status(),
            meta: Meta.t() | nil,
            sid: non_neg_integer() | nil,
            next_offset: non_neg_integer(),
            trim: non_neg_integer()
          }
  end

  @spec absent(binary()) :: t()
  def absent(path) do
    %__MODULE__{
      path: path,
      status: :absent,
      meta: nil,
      sid: nil,
      tail: 0,
      producers: %{},
      last_access: 0,
      expiry_key: nil,
      trim: 0
    }
  end

  @doc "A reserved creation that is hidden until activation."
  @spec pending(binary(), Meta.t()) :: t()
  def pending(path, %Meta{creation: %{__struct__: variant}, sid: sid} = meta)
      when variant in [ReferenceCreation, CopyCreation],
      do: %{absent(path) | status: :pending, meta: meta, sid: sid}

  @spec view(t()) :: View.t()
  def view(%__MODULE__{} = stream) do
    %View{
      status: stream.status,
      meta: stream.meta,
      sid: stream.sid,
      next_offset: stream.tail,
      trim: stream.trim
    }
  end

  @doc "The error for a request that needs an active stream."
  @spec error(:absent | :gone | :pending) :: {:error, :not_found | :gone | :unavailable}
  def error(:absent), do: {:error, :not_found}
  def error(:gone), do: {:error, :gone}
  def error(:pending), do: {:error, :unavailable}

  @doc "Whether an active stream has expired at `now` (PROTOCOL.md §5.1)."
  @spec expired?(t(), integer()) :: boolean()
  def expired?(%__MODULE__{meta: meta, last_access: last_access}, now) do
    (meta.expires_at_ms != nil and now >= meta.expires_at_ms) or
      (meta.ttl_s != nil and now >= last_access + meta.ttl_s * 1000)
  end

  @doc "The stream's expiry deadline, or `nil` if it does not expire."
  @spec deadline(t()) :: integer() | nil
  def deadline(%__MODULE__{meta: meta, last_access: last_access}), do: deadline(meta, last_access)

  @spec deadline(Meta.t(), integer()) :: integer() | nil
  def deadline(%Meta{expires_at_ms: at}, _last_access) when at != nil, do: at
  def deadline(%Meta{ttl_s: ttl}, last_access) when ttl != nil, do: last_access + ttl * 1000
  def deadline(_meta, _last_access), do: nil
end
