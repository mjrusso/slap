defmodule Slap.Streams.Store.Meta do
  @moduledoc false

  defstruct sid: nil,
            content_type: "application/octet-stream",
            created_ms: nil,
            ttl_s: nil,
            expires_at_ms: nil,
            closed: false,
            closed_by: nil,
            last_stream_seq: nil,
            fork_of: nil,
            forks: [],
            copying: false,
            soft_deleted: false

  @type t :: %__MODULE__{}
end

defmodule Slap.Streams.Store.ForkOf do
  @moduledoc false

  defstruct [
    :path,
    :offset,
    :requested_offset,
    :requested_content_type,
    :requested_ttl_s,
    :requested_expires_at_ms,
    sub_offset: 0
  ]

  @type t :: %__MODULE__{
          path: binary(),
          offset: non_neg_integer(),
          requested_offset: non_neg_integer() | nil,
          sub_offset: non_neg_integer(),
          requested_content_type: binary() | nil,
          requested_ttl_s: non_neg_integer() | nil,
          requested_expires_at_ms: integer() | nil
        }
end

defmodule Slap.Streams.Store.Tail do
  @moduledoc false
  defstruct next_offset: 0, last_access_ms: nil, expiry_key_ms: nil

  @type t :: %__MODULE__{
          next_offset: non_neg_integer(),
          last_access_ms: integer() | nil,
          expiry_key_ms: integer() | nil
        }
end

defmodule Slap.Streams.Store.Producer do
  @moduledoc false
  defstruct epoch: 0, last_seq: 0
  @type t :: %__MODULE__{epoch: non_neg_integer(), last_seq: non_neg_integer()}
end

defmodule Slap.Streams.Store.Codec do
  @moduledoc false

  alias Slap.Streams.Store.{ForkOf, Meta, Producer, Tail}

  @version 1

  # `:safe` decoding rejects atoms this node does not have yet, and a node
  # that takes over a shard may decode a stream's rows before it has loaded
  # any module that names their fields. The field names of every stored
  # struct are literals here (`atoms/0` returns them, so they are compiled
  # in), and loading this module creates them. So a stored field may hold
  # these structs, but no other atoms: no maps with atom keys.
  @atoms Enum.uniq(Enum.flat_map([%Meta{}, %ForkOf{}, %Tail{}, %Producer{}], &Map.keys/1))

  @doc false
  def atoms, do: @atoms

  @spec encode(Meta.t() | Tail.t() | Producer.t()) :: binary()
  def encode(%Meta{fork_of: %ForkOf{} = f} = v),
    do: pack(:meta, %{v | fork_of: Map.from_struct(f)})

  def encode(%Meta{} = v), do: pack(:meta, v)
  def encode(%Tail{} = v), do: pack(:tail, v)
  def encode(%Producer{} = v), do: pack(:producer, v)

  @spec decode(binary()) :: Meta.t() | Tail.t() | Producer.t()
  def decode(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      {:meta, @version, fields} -> meta(fields)
      {:tail, @version, fields} -> struct(Tail, fields)
      {:producer, @version, fields} -> struct(Producer, fields)
    end
  end

  defp meta(%{fork_of: %{} = fork_of} = fields),
    do: struct(Meta, %{fields | fork_of: struct(ForkOf, fork_of)})

  defp meta(fields), do: struct(Meta, fields)

  defp pack(tag, struct), do: :erlang.term_to_binary({tag, @version, Map.from_struct(struct)})
end
