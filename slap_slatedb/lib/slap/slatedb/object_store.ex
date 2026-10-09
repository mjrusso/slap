defmodule Slap.SlateDB.ObjectStore do
  @moduledoc """
  Plain objects in the store a database lives in: small objects with
  conditional writes (`get/3`, `put/4`), such as leases (see
  `Slap.Cluster.Strategy.ObjectLease`), and large ones streamed in and out
  (`upload/4`, `download/3`), such as file bodies. Every call takes the
  `:timeout` option (see `Slap.SlateDB`); for a stream it bounds each step.

      {:ok, store} = Slap.SlateDB.ObjectStore.open("_cluster", store: {:url, "s3://bucket/prefix"})
      {:ok, version} = Slap.SlateDB.ObjectStore.put(store, "leases/0", "a", mode: :create)
      {:ok, {"a", ^version}} = Slap.SlateDB.ObjectStore.get(store, "leases/0")
      {:error, :conflict} = Slap.SlateDB.ObjectStore.put(store, "leases/0", "b", mode: :create)
      {:ok, _} = Slap.SlateDB.ObjectStore.put(store, "leases/0", "b", mode: {:update, version})

  Conditional updates (`{:update, version}`, an `If-Match` PUT) need a store
  that supports them: S3 and S3-compatible stores such as RustFS, Azure,
  GCS, and `:memory`. The local file system only supports `:create`, and
  returns `{:error, :unsupported}` for an update.

  An atom error means the store refused a condition or does not support it;
  `%Slap.SlateDB.Error{}` means the operation failed.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Native, Options}

  # The NIF takes offsets as u64.
  @max_offset 0xFFFF_FFFF_FFFF_FFFF

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}
  @typedoc "An object's version: opaque, compare it only for equality."
  @opaque version :: {String.t() | nil, String.t() | nil}

  @doc """
  Opens `store` (as for `Slap.SlateDB.open/2`). Keys are relative to `path`
  within it (and to the prefix of a store URL). The `:store` option is required.

  A `:memory` store is new and empty each time it is opened.
  """
  @spec open(String.t(), keyword()) :: {:ok, t()} | {:error, SlateDB.Error.t()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    Keyword.validate!(opts, [:store, :timeout])
    store = Options.store(Keyword.fetch!(opts, :store))

    with {:ok, resource} <-
           Native.call(&Native.objstore_open(store, path, &1), Native.timeout(opts)) do
      {:ok, %__MODULE__{resource: resource}}
    end
  end

  @doc "The object's body and version, or `{:ok, nil}` if there is none."
  @spec get(t(), String.t(), keyword()) ::
          {:ok, {binary(), version()} | nil} | {:error, SlateDB.Error.t()}
  def get(%__MODULE__{resource: res}, key, opts \\ []) when is_binary(key) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.objstore_get(res, key, &1), Native.timeout(opts))
  end

  @doc """
  Writes an object. `:mode` is:

    * `:overwrite` (default) - unconditionally.
    * `:create` - only if there is no object at `key`.
    * `{:update, version}` - only if the object is still at `version`.

  Returns `{:ok, version}`, `{:error, :conflict}` when the condition does not
  hold, or `{:error, :unsupported}` when the store cannot check it.
  """
  @spec put(t(), String.t(), binary(), keyword()) ::
          {:ok, version()} | {:error, :conflict | :unsupported | SlateDB.Error.t()}
  def put(%__MODULE__{resource: res}, key, body, opts \\ [])
      when is_binary(key) and is_binary(body) do
    Keyword.validate!(opts, [:mode, :timeout])
    mode = put_mode(Keyword.get(opts, :mode, :overwrite))

    case Native.call(&Native.objstore_put(res, key, body, mode, &1), Native.timeout(opts)) do
      {:ok, {:ok, version}} -> {:ok, version}
      {:ok, :conflict} -> {:error, :conflict}
      {:ok, :unsupported} -> {:error, :unsupported}
      {:error, _} = error -> error
    end
  end

  defp put_mode(mode) when mode in [:overwrite, :create], do: mode
  defp put_mode({:update, {_, _}} = mode), do: mode
  defp put_mode(other), do: raise(ArgumentError, "invalid :mode, got: #{inspect(other)}")

  @doc "Deletes an object. Deleting one that does not exist is not an error."
  @spec delete(t(), String.t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def delete(%__MODULE__{resource: res}, key, opts \\ []) when is_binary(key) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.objstore_delete(res, key, &1), Native.timeout(opts))
  end

  # A body is streamed in parts of this size (S3's minimum, but the last).
  @part_size 5 * 1024 * 1024

  @doc """
  Writes the object at `key` from `chunks`, an enumerable of binaries, and
  returns `{:ok, version}`. Nothing is visible at `key` until the whole body
  is written.

  A body smaller than one part (5 MiB) is written with a single PUT; a
  larger one as a multipart upload, a few parts at a time, which is aborted
  if `chunks` raises or the upload fails. A process that dies during an
  upload leaves its parts behind until the store expires them (S3 needs a
  lifecycle rule for incomplete multipart uploads).
  """
  @spec upload(t(), String.t(), Enumerable.t(), keyword()) ::
          {:ok, version()} | {:error, SlateDB.Error.t()}
  def upload(%__MODULE__{} = store, key, chunks, opts \\ []) when is_binary(key) do
    Keyword.validate!(opts, [:timeout])
    # Chunks are pulled one at a time (`chunks` may be a stream that can
    # only be read once), so an upload in progress can be aborted when a
    # write fails or `chunks` raises.
    next = &Enumerable.reduce(chunks, &1, fn chunk, _ -> {:suspend, chunk} end)
    buffer(store, key, next, [], 0, opts)
  end

  # Up to a part is buffered: a body that ends first is a single PUT.
  defp buffer(store, key, next, acc, size, opts) do
    case next.({:cont, nil}) do
      {:suspended, chunk, next} when size + byte_size(chunk) < @part_size ->
        buffer(store, key, next, [acc, chunk], size + byte_size(chunk), opts)

      {:suspended, chunk, next} ->
        timeout = Native.timeout(opts)

        case Native.call(&Native.objstore_upload_open(store.resource, key, &1), timeout) do
          {:ok, upload} ->
            send_part(upload, next, [acc, chunk], timeout)

          {:error, _} = error ->
            next.({:halt, nil})
            error
        end

      # A stream that ends by itself reports :halted.
      {finished, _} when finished in [:done, :halted] ->
        put(store, key, IO.iodata_to_binary(acc), opts)
    end
  end

  defp send_part(upload, next, data, timeout) do
    data = IO.iodata_to_binary(data)

    case Native.call(&Native.objstore_upload_write(upload, data, &1), timeout) do
      :ok ->
        case pull(upload, next, timeout) do
          {:suspended, chunk, next} -> send_part(upload, next, chunk, timeout)
          {finished, _} when finished in [:done, :halted] -> finish(upload, timeout)
        end

      {:error, _} = error ->
        next.({:halt, nil})
        abort(upload, error, timeout)
    end
  end

  defp pull(upload, next, timeout) do
    next.({:cont, nil})
  catch
    kind, reason ->
      abort(upload, :ok, timeout)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp finish(upload, timeout) do
    case Native.call(&Native.objstore_upload_finish(upload, &1), timeout) do
      {:ok, version} -> {:ok, version}
      {:error, _} = error -> abort(upload, error, timeout)
    end
  end

  defp abort(upload, result, timeout) do
    Native.call(&Native.objstore_upload_abort(upload, &1), timeout)
    result
  end

  @doc """
  Opens the object at `key` for reading. Returns `{:ok, {chunks, size,
  version}}`, where `chunks` is a lazy stream of binaries (reading it
  raises `Slap.SlateDB.Error` if the store fails) and `size` is the size of
  the whole object, or `{:ok, nil}` if there is no object.

  `range: {first, last}` reads only the bytes from `first` to `last`,
  inclusive, as an HTTP range does. A `last` past the end of the object
  reads to its end. A range that starts past the end returns an error whose
  form depends on the store, so check the range against the object's size
  first.
  """
  @spec download(t(), String.t(), keyword()) ::
          {:ok, {Enumerable.t(), non_neg_integer(), version()} | nil}
          | {:error, SlateDB.Error.t()}
  def download(%__MODULE__{resource: res}, key, opts \\ []) when is_binary(key) do
    Keyword.validate!(opts, [:timeout, :range])
    timeout = Native.timeout(opts)
    range = download_range(Keyword.get(opts, :range))

    case Native.call(&Native.objstore_download_open(res, key, range, &1), timeout) do
      {:ok, {download, size, version}} -> {:ok, {chunks(download, timeout), size, version}}
      other -> other
    end
  end

  defp download_range(nil), do: nil

  defp download_range({first, last} = range)
       when is_integer(first) and is_integer(last) and first >= 0 and last >= first and
              last <= @max_offset,
       do: range

  defp download_range(other),
    do: raise(ArgumentError, "invalid :range, got: #{inspect(other)}")

  defp chunks(download, timeout) do
    Stream.unfold(download, fn download ->
      case Native.call(&Native.objstore_download_next(download, &1), timeout) do
        {:ok, :eof} -> nil
        {:ok, chunk} -> {chunk, download}
        {:error, error} -> raise error
      end
    end)
  end

  @doc "The keys under `prefix`, in no particular order."
  @spec list(t(), String.t(), keyword()) :: {:ok, [String.t()]} | {:error, SlateDB.Error.t()}
  def list(%__MODULE__{resource: res}, prefix, opts \\ []) when is_binary(prefix) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.objstore_list(res, prefix, &1), Native.timeout(opts))
  end
end
