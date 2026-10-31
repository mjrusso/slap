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
    case parse_term(:erlang.binary_to_term(binary, [:safe])) do
      {:ok, record} -> record
    end
  end

  @doc """
  Parses a stored record without raising. `to_term` converts the binary to
  `{:ok, term}` or an error. It is called from this module, so the record
  atoms exist for a `:safe` conversion.
  """
  def parse(binary, to_term) do
    with {:ok, term} <- to_term.(binary), do: parse_term(term)
  end

  defp parse_term({:meta, 2, fields}), do: build(Meta, fields)
  defp parse_term({:tail, 1, fields}), do: build(Tail, fields)
  defp parse_term({:producer, 1, fields}), do: build(Producer, fields)
  defp parse_term({:capture, 1, fields}), do: build(Capture, fields)

  defp parse_term({:meta, 1, _fields}),
    do: {:error, :metadata_v1_requires_offline_upgrade}

  defp parse_term({_tag, version, _fields}), do: {:error, {:unsupported_version, version}}
  defp parse_term(_term), do: {:error, :malformed_record}

  defp pack(tag, version, value),
    do: :erlang.term_to_binary({tag, version, encode_fields(Map.from_struct(value))})

  defp encode_fields(fields),
    do: Map.new(fields, fn {key, value} -> {key, encode_value(value)} end)

  defp encode_value(%module{} = value),
    do: {Map.fetch!(@nested_tags, module), encode_fields(Map.from_struct(value))}

  defp encode_value(fields) when is_map(fields), do: encode_fields(fields)
  defp encode_value(value), do: value

  # Introspection parses corrupt records, so a field set that `struct!/2`
  # would reject is an error here.
  defp build(module, fields) do
    with {:ok, fields} <- decode_fields(fields) do
      spec = module.__info__(:struct)
      names = Enum.map(spec, & &1.field)

      if Enum.all?(Map.keys(fields), &(&1 in names)) and
           Enum.all?(spec, &(not &1.required or Map.has_key?(fields, &1.field))),
         do: {:ok, struct!(module, fields)},
         else: {:error, :malformed_record}
    end
  end

  defp decode_fields(fields) when is_map(fields) do
    Enum.reduce_while(fields, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case decode_value(value) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp decode_fields(_fields), do: {:error, :malformed_record}

  defp decode_value({tag, fields}) when is_atom(tag) and is_map(fields) do
    case Map.fetch(@nested_types, tag) do
      {:ok, module} -> build(module, fields)
      :error -> {:error, :malformed_record}
    end
  end

  defp decode_value(%_{}), do: {:error, :malformed_record}
  defp decode_value(fields) when is_map(fields), do: decode_fields(fields)
  defp decode_value(value), do: {:ok, value}
end
