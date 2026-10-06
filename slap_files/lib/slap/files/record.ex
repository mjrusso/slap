defmodule Slap.Files.Record do
  @moduledoc false
  # A file's row in Slap.KV: the namespace's record partition, with key
  # the ref's id. It exists exactly while the file does, and holds the
  # file's metadata and body: {:inline, bytes} or {:object, key}.

  alias Slap.Files.{Config, Deadline, File}
  alias Slap.KV

  @type t :: %{
          body: {:inline, binary()} | {:object, String.t()},
          size: non_neg_integer(),
          sha256: binary(),
          content_type: String.t(),
          metadata: %{String.t() => String.t()}
        }

  @spec get(Slap.Files.ref(), keyword(), Config.t()) ::
          {:ok, {KV.version(), t()} | nil} | {:error, term()}
  def get({partition, id}, opts, config) do
    case KV.get(partition(partition, config), id, opts) do
      {:ok, nil} -> {:ok, nil}
      {:ok, %{value: value, version: version}} -> {:ok, {version, decode(value)}}
      {:error, _} = error -> error
    end
  end

  # `if_version` is the version the row must be at, or :absent. `due_ms` is
  # when the intent that covers the write is due (see Slap.Files.Deadline).
  @spec put(Slap.Files.ref(), t(), KV.version() | :absent, integer() | nil, Config.t()) ::
          {:ok, KV.version()} | {:error, term()}
  def put({partition, id}, record, if_version, due_ms, config) do
    opts =
      [if_version: if_version] ++
        Config.route_opts(config) ++ Deadline.opts(due_ms, config)

    Deadline.result(KV.put(partition(partition, config), id, encode(record), opts))
  end

  @spec delete(Slap.Files.ref(), KV.version(), integer() | nil, Config.t()) ::
          :ok | {:error, term()}
  def delete({partition, id}, version, due_ms, config) do
    opts =
      [if_version: version] ++
        Config.route_opts(config) ++ Deadline.opts(due_ms, config)

    Deadline.result(KV.delete(partition(partition, config), id, opts))
  end

  @spec list(binary(), keyword(), Config.t()) ::
          {:ok, %{rows: [{binary(), KV.version(), t()}], cursor: binary() | nil}}
          | {:error, term()}
  def list(partition, opts, config) do
    with {:ok, page} <- KV.scan(partition(partition, config), [{:with_versions, true} | opts]) do
      {:ok,
       %{page | rows: for({id, value, version} <- page.rows, do: {id, version, decode(value)})}}
    end
  end

  @spec object_key(t() | nil) :: String.t() | nil
  def object_key(%{body: {:object, key}}), do: key
  def object_key(_record), do: nil

  @spec same?(t(), t()) :: boolean()
  def same?(a, b),
    do:
      Map.take(a, [:sha256, :size, :content_type, :metadata]) ==
        Map.take(b, [:sha256, :size, :content_type, :metadata]) and
        elem(a.body, 0) == elem(b.body, 0)

  @spec to_file(Slap.Files.ref(), KV.version(), t()) :: File.t()
  def to_file(ref, version, record) do
    %File{
      ref: ref,
      version: version,
      size: record.size,
      sha256: record.sha256,
      content_type: record.content_type,
      metadata: record.metadata,
      storage: elem(record.body, 0)
    }
  end

  defp partition(partition, config), do: Config.partition(config, :record, partition)

  defp encode(record), do: :erlang.term_to_binary({:file, 1, record})

  defp decode(binary) do
    {:file, 1, record} = :erlang.binary_to_term(binary, [:safe])
    record
  end
end
