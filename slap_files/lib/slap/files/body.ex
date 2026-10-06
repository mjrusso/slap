defmodule Slap.Files.Body do
  @moduledoc false
  # Where a body goes, and how it gets there. A body is a binary or an
  # enumerable of binaries; an enumerable is read once, one chunk at a time
  # (it may be a request body), and buffered only as far as the choice
  # between inline and object needs.

  alias Slap.Files.Body.InvalidChunkError
  alias Slap.Files.Config
  alias Slap.SlateDB.ObjectStore

  @type prepared :: {:inline, binary()} | {:object, Enumerable.t()}

  @spec prepare(binary() | Enumerable.t(), :auto | :inline | :object, Config.t()) ::
          {:ok, prepared()} | {:error, term()}
  def prepare(body, :object, _config) when is_binary(body), do: {:ok, {:object, [body]}}
  def prepare(body, :object, _config), do: {:ok, {:object, body}}

  def prepare(body, :inline, config) when is_binary(body) do
    if byte_size(body) <= config.inline_limit,
      do: {:ok, {:inline, body}},
      else: {:error, :too_large_for_inline}
  end

  def prepare(body, :auto, config) when is_binary(body) do
    if byte_size(body) <= config.inline_max_bytes,
      do: {:ok, {:inline, body}},
      else: {:ok, {:object, [body]}}
  end

  def prepare(chunks, storage, config) when storage in [:auto, :inline] do
    limit = if storage == :inline, do: config.inline_limit, else: config.inline_max_bytes
    next = &Enumerable.reduce(chunks, &1, fn chunk, _ -> {:suspend, chunk} end)

    case buffer(next, [], 0, limit) do
      {:error, _reason} = error ->
        error

      {:all, acc} ->
        {:ok, {:inline, IO.iodata_to_binary(acc)}}

      {:more, _acc, next} when storage == :inline ->
        next.({:halt, nil})
        {:error, :too_large_for_inline}

      {:more, acc, next} ->
        {:ok, {:object, rest(acc, next)}}
    end
  end

  def prepare(_body, storage, _config), do: {:error, {:bad_request, {:invalid_storage, storage}}}

  defp buffer(next, acc, size, limit) do
    case next.({:cont, nil}) do
      {:suspended, chunk, next} when is_binary(chunk) and size + byte_size(chunk) <= limit ->
        buffer(next, [acc, chunk], size + byte_size(chunk), limit)

      {:suspended, chunk, next} when is_binary(chunk) ->
        {:more, [acc, chunk], next}

      {:suspended, _chunk, next} ->
        next.({:halt, nil})
        {:error, {:bad_request, :invalid_body}}

      # A stream that ends by itself reports :halted.
      {finished, _} when finished in [:done, :halted] ->
        {:all, acc}
    end
  end

  defp rest(acc, next) do
    Stream.resource(
      fn -> {:buffered, acc, next} end,
      fn
        {:buffered, acc, next} ->
          {[IO.iodata_to_binary(acc)], next}

        :done ->
          {:halt, :done}

        next ->
          case next.({:cont, nil}) do
            {:suspended, chunk, next} -> {[chunk], next}
            {_finished, _} -> {:halt, :done}
          end
      end,
      fn
        # Stopped early (the upload failed): let the enumerable clean up.
        {:buffered, _acc, next} -> next.({:halt, nil})
        next when is_function(next, 1) -> next.({:halt, nil})
        :done -> :ok
      end
    )
  end

  @spec upload(ObjectStore.t(), String.t(), Enumerable.t(), keyword()) ::
          {:ok, non_neg_integer(), binary()} | {:error, term()}
  def upload(objects, key, chunks, opts \\ []) do
    # The upload reads `chunks` in this process: count and hash them as it
    # goes.
    tag = {__MODULE__, make_ref()}
    Process.put(tag, {:crypto.hash_init(:sha256), 0})

    hashed =
      Stream.each(chunks, fn chunk ->
        unless is_binary(chunk), do: raise(InvalidChunkError)
        {hash, size} = Process.get(tag)
        Process.put(tag, {:crypto.hash_update(hash, chunk), size + byte_size(chunk)})
      end)

    try do
      with {:ok, _version} <- ObjectStore.upload(objects, key, hashed, opts) do
        {hash, size} = Process.get(tag)
        {:ok, size, :crypto.hash_final(hash)}
      end
    rescue
      InvalidChunkError -> {:error, {:bad_request, :invalid_body}}
    after
      Process.delete(tag)
    end
  end
end
