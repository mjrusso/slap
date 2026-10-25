defmodule Slap.Streams.Store.Meta do
  @moduledoc false

  alias Slap.Streams.Store.{CopyCreation, Creation, PublishedReference, ReferenceCreation}

  defstruct sid: nil,
            content_type: "application/octet-stream",
            created_ms: nil,
            ttl_s: nil,
            expires_at_ms: nil,
            closed: false,
            closed_by: nil,
            last_stream_seq: nil,
            soft_deleted: false,
            creation: nil,
            owner: nil,
            registration_count: 0,
            pin_start: nil,
            reclaimed_before: 0

  @type t :: %__MODULE__{creation: Creation.t() | nil}

  def pending?(%__MODULE__{creation: %ReferenceCreation{}}), do: true
  def pending?(%__MODULE__{creation: %CopyCreation{}}), do: true
  def pending?(_meta), do: false

  def reference(%__MODULE__{creation: %PublishedReference{capture: cap}}), do: cap
  def reference(_meta), do: nil

  def depth(%__MODULE__{creation: %PublishedReference{capture: cap}}), do: cap.depth
  def depth(_meta), do: 0
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

  alias Slap.Streams.Store.{
    Capture,
    CopyCreation,
    CopyProgress,
    Identity,
    Meta,
    Producer,
    PublishedCopy,
    PublishedReference,
    ReferenceCreation,
    Tail
  }

  @nested_types %{
    identity: Identity,
    capture: Capture,
    reference_pending: ReferenceCreation,
    copy_pending: CopyCreation,
    copy_progress: CopyProgress,
    reference: PublishedReference,
    copy: PublishedCopy
  }
  @nested_tags Map.new(@nested_types, fn {tag, module} -> {module, tag} end)

  # Safe ETF decoding may run before the creation modules load. Load every
  # stored tag and field atom without putting module names in the record.
  @atoms Enum.uniq(
           [
             :body,
             :forked_from,
             :source_placement_key,
             :fork_offset,
             :fork_sub_offset,
             :guard,
             :identity,
             :offset
           ] ++
             Map.keys(@nested_types) ++
             Enum.flat_map(
               [Meta, Tail, Producer | Map.values(@nested_types)],
               &Map.keys(Map.from_struct(struct(&1)))
             )
         )

  @doc false
  def atoms, do: @atoms

  @spec encode(Meta.t() | Tail.t() | Producer.t() | Capture.t()) :: binary()
  def encode(%Meta{} = v), do: pack(:meta, 2, v)
  def encode(%Tail{} = v), do: pack(:tail, 1, v)
  def encode(%Producer{} = v), do: pack(:producer, 1, v)
  def encode(%Capture{} = v), do: pack(:capture, 1, v)

  @spec decode(binary()) :: Meta.t() | Tail.t() | Producer.t() | Capture.t()
  def decode(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      {:meta, 2, fields} ->
        struct!(Meta, decode_fields(fields))

      {:tail, 1, fields} ->
        struct!(Tail, decode_fields(fields))

      {:producer, 1, fields} ->
        struct!(Producer, decode_fields(fields))

      {:capture, 1, fields} ->
        struct!(Capture, decode_fields(fields))
    end
  end

  defp pack(tag, version, value),
    do: :erlang.term_to_binary({tag, version, encode_fields(Map.from_struct(value))})

  defp encode_fields(fields),
    do: Map.new(fields, fn {key, value} -> {key, encode_value(value)} end)

  defp encode_value(%module{} = value),
    do: {Map.fetch!(@nested_tags, module), encode_fields(Map.from_struct(value))}

  defp encode_value(fields) when is_map(fields), do: encode_fields(fields)
  defp encode_value(value), do: value

  defp decode_fields(fields),
    do: Map.new(fields, fn {key, value} -> {key, decode_value(value)} end)

  defp decode_value({tag, fields}) when is_atom(tag) and is_map(fields),
    do: struct!(Map.fetch!(@nested_types, tag), decode_fields(fields))

  defp decode_value(%_{}), do: raise(ArgumentError, "untagged stored struct")
  defp decode_value(fields) when is_map(fields), do: decode_fields(fields)
  defp decode_value(value), do: value
end
