defmodule Slap.Files do
  @moduledoc """
  Files stored with `Slap.KV` and an object store. A file lives at a ref,
  `{partition, id}`: a partition is a stable group of files (a document's,
  for example), listed together, and on one `Slap.KV` shard. Each file has
  one record in `Slap.KV`, which exists exactly while the file does, and a
  body stored in the record (inline) or as an object.

  Files can be replaced; bodies never are. Each `put/3` writes a new body,
  switches the file's record to it with a conditional write, and gives the
  file a new version. The old body is deleted after `retention_ms`, so a
  reader that already has it can finish.

  Start it with the object store for bodies, after `Slap.KV.Cluster`:

      {Slap.Files, store: {:url, "s3://bucket/prefix"}, path: "files"}

  Options:

    * `:store` - required, as for `Slap.SlateDB.open/2`; `:path` - where the
      objects go in it (default `"files"`).
    * `:inline_max_bytes` - bodies up to this size are stored inline with
      `storage: :auto` (default 16 KiB).
    * `:inline_limit` - the largest inline body, even with `storage:
      :inline` (default 1 MiB).
    * `:retention_ms` - how long an old body is kept (default 5 minutes).
    * `:upload_timeout_ms` - how long an upload may take before its object
      may be deleted (default 1 hour).
    * `:sweep_interval_ms` - see `Slap.Files.Sweeper` (default 10 s).
    * `:max_clock_skew_ms` - how far apart the nodes' clocks may be (default
      30 s): see `Slap.Files.Sweeper`.
    * `:reconcile_interval_ms` - see `Slap.Files.Sweeper` (default 1 hour).
    * `:name` - instance name (default `Slap.Files`). Pass it as `files:`
      to file operations.
    * `:cluster` - the instance's KV cluster (default `Slap.KV.Cluster`);
      define an application-owned one with `use Slap.KV.Cluster, otp_app: :my_app`.
    * `:namespace` - stable data namespace within the KV cluster and object
      store (default `"default"`). Named instances require an explicit
      namespace so renaming an instance does not change its stored layout.
      Changing the namespace leaves existing files at the old layout.
      Instances with the same namespace share records, intents, and object
      registrations.
    * `:timeout` - timeout for each KV or object-store call (default no
      timeout for object-store calls); operations can override it.

  `:clock` is an internal test hook.

  ## Errors

  Invalid file data (`ref`, body, metadata, content type, expected SHA-256 and
  `:if_version`) returns an error tuple. Invalid control options (`:storage`,
  `:files`, `:timeout`) and unknown option names raise `ArgumentError`.

    * `{:error, {:conflict, version}}` - an `if_version:` condition does not
      hold; `version` is the file's current one, or nil.
    * `{:error, :expired}` - an upload took longer than
      `upload_timeout_ms`; its object is deleted.
    * `{:error, :checksum_mismatch}`, `{:error, :too_large_for_inline}`,
      `{:error, {:bad_request, reason}}`.
    * `{:error, :unavailable | :timeout}` - from `Slap.KV` or the store. A
      write may still have been applied; `get/1` shows whether it was.

  ## Telemetry

  `put/3`, `delete/2`, `get/2`, `read/2`, `stream/2`, and `list/2` emit
  Telemetry spans under `[:slap, :files, operation]`, with `:start`, `:stop`,
  and `:exception` events. Metadata includes `:files`, the instance name;
  a stop event also has `:outcome` (`:ok` or `:error`). The `stream/2` span
  measures opening the body stream; consuming it happens after that span ends.
  """

  use Supervisor

  alias Slap.Files.{Body, Config, Intent, Object, Record, Sweeper}
  alias Slap.SlateDB.Error
  alias Slap.SlateDB.ObjectStore

  @type ref :: {partition :: binary(), id :: binary()}
  @type error ::
          Slap.KV.error()
          | :checksum_mismatch
          | :expired
          | :too_large_for_inline

  @retries 3
  @start_options [
    :store,
    :path,
    :timeout,
    :name,
    :cluster,
    :namespace,
    :inline_max_bytes,
    :inline_limit,
    :retention_ms,
    :upload_timeout_ms,
    :sweep_interval_ms,
    :max_clock_skew_ms,
    :reconcile_interval_ms,
    :clock
  ]

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    validate_options!(opts, @start_options)
    Config.validate_options!(opts)

    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    validate_options!(opts, @start_options)
    Config.validate_options!(opts)
    name = Keyword.get(opts, :name, __MODULE__)

    case ObjectStore.open(
           Keyword.get(opts, :path, "files"),
           [store: Keyword.fetch!(opts, :store)] ++
             if(opts[:timeout], do: [timeout: opts[:timeout]], else: [])
         ) do
      {:ok, objects} -> Supervisor.start_link(__MODULE__, {opts, objects}, name: name)
      {:error, error} -> {:error, {:store, error}}
    end
  end

  @impl true
  def init({opts, objects}) do
    config = Config.new(objects, opts)
    Supervisor.init([{Config, config}, {Sweeper, config: config}], strategy: :one_for_one)
  end

  @doc "A new random id, for a ref."
  @spec new_id() :: binary()
  def new_id, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  @doc """
  Writes a file: creates it, or replaces its body and metadata. `body` is a
  binary or an enumerable of binaries (read once). Returns the file's new
  `Slap.Files.File` once it is durable.

  Options:

    * `:if_version` - `:absent` to create only, or the version the file
      must be at.
    * `:storage` - `:auto` (default), `:inline` or `:object`.
    * `:content_type` - default `"application/octet-stream"`.
    * `:metadata` - a map of strings to strings (default `%{}`).
    * `:expected_sha256` - the body's expected SHA-256 (32 bytes).
    * `:files` - the instance name (default `Slap.Files`).
    * `:timeout` - overrides the instance's timeout for each KV or
      object-store call.

  Writing the body, content type, metadata and storage the file already has
  without a condition returns the file as it is. With `:if_version`, the
  condition must hold even when the content is unchanged.
  """
  @spec put(ref(), binary() | Enumerable.t(), keyword()) ::
          {:ok, Slap.Files.File.t()} | {:error, error()}
  def put(ref, body, opts \\ []), do: span(:put, opts, fn -> do_put(ref, body, opts) end)

  defp do_put(ref, body, opts) do
    validate_options!(opts, [
      :if_version,
      :storage,
      :content_type,
      :metadata,
      :expected_sha256,
      :files,
      :timeout
    ])

    validate_request_opts!(opts)

    config = request_config(opts)

    with :ok <- validate_ref(ref),
         :ok <- validate_body(body),
         {:ok, condition} <- condition(opts, true),
         {:ok, meta} <- meta(opts),
         {:ok, prepared} <- Body.prepare(body, Keyword.get(opts, :storage, :auto), config) do
      write(ref, prepared, meta, condition, opts, config)
    end
  end

  defp write(ref, {:inline, bytes}, meta, condition, opts, config) do
    record =
      Map.merge(meta, %{
        body: {:inline, bytes},
        size: byte_size(bytes),
        sha256: :crypto.hash(:sha256, bytes)
      })

    with :ok <- verify(record, opts), do: commit(ref, record, condition, nil, @retries, config)
  end

  defp write(ref, {:object, chunks}, meta, condition, opts, config) do
    key = Object.new_key(ref, config)

    # The intent comes first: an upload that stops half-way leaves an
    # object only it names. Then the key's registration, which the sweep
    # of the intent deletes (see Slap.Files.Object). Should registering
    # fail, the intent is left due as it is, not discarded: until then the
    # registration may still be applied.
    with {:ok, intent} <-
           Intent.open(ref, [key], Config.now(config) + config.upload_timeout_ms, config),
         :ok <- Object.register(key, intent.due_ms, config) do
      with {:ok, size, sha} <-
             Body.upload(config.objects, key, chunks, Config.object_opts(config)),
           record = Map.merge(meta, %{body: {:object, key}, size: size, sha256: sha}),
           :ok <- verify(record, opts) do
        commit(ref, record, condition, intent, @retries, config)
      else
        {:error, %Error{} = error} ->
          discard(intent, config)
          object_error(error)

        error ->
          discard(intent, config)
          error
      end
    end
  end

  defp commit(ref, record, condition, intent, retries, config) do
    with {:ok, current} <- Record.get(ref, Config.route_opts(config), config) do
      cond do
        not holds?(current, condition) ->
          discard(intent, config)
          {:error, {:conflict, version_of(current)}}

        current != nil and Record.same?(elem(current, 1), record) ->
          discard(intent, config)
          {:ok, Record.to_file(ref, elem(current, 0), elem(current, 1))}

        true ->
          switch(ref, record, condition, intent, current, retries, config)
      end
    end
  end

  defp switch(ref, record, condition, intent, current, retries, config) do
    old_key = current && Record.object_key(elem(current, 1))
    keys = Enum.reject([Record.object_key(record), old_key], &is_nil/1)

    with {:ok, intent} <- cover(intent, ref, keys, config) do
      case Record.put(ref, record, if_version(current), due(intent), config) do
        {:ok, version} ->
          done(intent, old_key)
          {:ok, Record.to_file(ref, version, record)}

        {:error, {:conflict, _}} when condition == :any and retries > 0 ->
          commit(ref, record, condition, intent, retries - 1, config)

        {:error, {:conflict, version}} ->
          discard(intent, config)
          {:error, {:conflict, version}}

        # The outcome is unknown: the intent stays, and the sweeper deletes
        # whichever body the record does not point to.
        {:error, _} = error ->
          error
      end
    end
  end

  # Makes an intent name `keys`, due after the retention period: the old
  # body is deleted then, and the new one too if the record never points to
  # it. Taking the upload's intent fails if the sweeper took it first.
  defp cover(nil, _ref, [], _config), do: {:ok, nil}

  defp cover(nil, ref, keys, config), do: Intent.open(ref, keys, retention_due(config), config)

  defp cover(intent, _ref, keys, config) do
    case Intent.take(intent,
           keys: Enum.uniq(intent.keys ++ keys),
           due_ms: retention_due(config)
         ) do
      {:ok, intent} ->
        {:ok, intent}

      # The upload was too slow: the sweeper took its intent, and deletes
      # its object. Should the upload write it after that, reconciliation
      # deletes it (see Slap.Files.Object).
      {:error, :taken} ->
        {:error, :expired}

      {:error, _} = error ->
        error
    end
  end

  # Without an old body to delete later, the upload's intent is done. The
  # caller does not wait: should this fail, the sweeper deletes the intent
  # when it is due (the object is the file's body by then, and stays).
  defp done(nil, _old_key), do: :ok
  defp done(intent, nil), do: Task.start(fn -> Intent.done(intent) end)
  defp done(_intent, _old_key), do: :ok

  defp discard(nil, _config), do: :ok
  defp discard(intent, config), do: Intent.take(intent, due_ms: Config.now(config))

  defp retention_due(config), do: Config.now(config) + config.retention_ms

  # A write that an intent covers is not applied once the intent is due.
  defp due(nil), do: nil
  defp due(intent), do: intent.due_ms

  @doc """
  Deletes a file. Returns `:ok` once it is gone (also if there was none,
  without `:if_version`); its body is deleted after `retention_ms`.
  `if_version:` makes it conditional, as for `put/3`.
  """
  @spec delete(ref(), keyword()) :: :ok | {:error, error()}
  def delete(ref, opts \\ []), do: span(:delete, opts, fn -> do_delete(ref, opts) end)

  defp do_delete(ref, opts) do
    validate_options!(opts, [:if_version, :files, :timeout])
    validate_request_opts!(opts)
    config = request_config(opts)

    with :ok <- validate_ref(ref),
         {:ok, condition} <- condition(opts, false) do
      delete(ref, condition, @retries, config)
    end
  end

  defp delete(ref, condition, retries, config) do
    with {:ok, current} <- Record.get(ref, Config.route_opts(config), config),
         do: delete(ref, condition, current, retries, config)
  end

  defp delete(_ref, :any, nil, _retries, _config), do: :ok

  defp delete(ref, condition, current, retries, config) do
    if holds?(current, condition),
      do: remove(ref, condition, current, retries, config),
      else: {:error, {:conflict, version_of(current)}}
  end

  # The intent comes first: the body is deleted once the retention period
  # is over, if the record is gone by then.
  defp remove(ref, condition, {version, record}, retries, config) do
    with {:ok, intent} <- cover(nil, ref, List.wrap(Record.object_key(record)), config) do
      case Record.delete(ref, version, due(intent), config) do
        {:error, {:conflict, _}} when condition == :any and retries > 0 ->
          delete(ref, condition, retries - 1, config)

        other ->
          other
      end
    end
  end

  @doc "The file's metadata, or `{:ok, nil}`."
  @spec get(ref(), keyword()) :: {:ok, Slap.Files.File.t() | nil} | {:error, error()}
  def get(ref, opts \\ []), do: span(:get, opts, fn -> do_get(ref, opts) end)

  defp do_get(ref, opts) do
    validate_options!(opts, [:files, :timeout])
    validate_request_opts!(opts)
    config = request_config(opts)

    with :ok <- validate_ref(ref),
         {:ok, current} <- Record.get(ref, Config.route_opts(config), config) do
      case current do
        nil -> {:ok, nil}
        {version, record} -> {:ok, Record.to_file(ref, version, record)}
      end
    end
  end

  @doc "The file's body, or `{:ok, nil}`. See `stream/1` for large bodies."
  @spec read(ref(), keyword()) :: {:ok, binary() | nil} | {:error, error()}
  def read(ref, opts \\ []), do: span(:read, opts, fn -> do_read(ref, opts) end)

  defp do_read(ref, opts) do
    case do_stream(ref, opts) do
      {:ok, nil} -> {:ok, nil}
      {:ok, {_file, chunks}} -> read_chunks(chunks)
      error -> error
    end
  end

  defp read_chunks(chunks) do
    {:ok, chunks |> Enum.to_list() |> IO.iodata_to_binary()}
  rescue
    error in Error -> object_error(error)
  end

  @doc """
  The file and its body as a lazy stream of binaries, or `{:ok, nil}`. An
  object body stays readable for `retention_ms` after the file is replaced
  or deleted. A store failure while reading the returned stream raises
  `Slap.SlateDB.Error`; failure to open the download returns
  `{:error, :unavailable | :timeout}`.
  """
  @spec stream(ref(), keyword()) ::
          {:ok, {Slap.Files.File.t(), Enumerable.t()} | nil} | {:error, error()}
  def stream(ref, opts \\ []), do: span(:stream, opts, fn -> do_stream(ref, opts) end)

  defp do_stream(ref, opts) do
    validate_options!(opts, [:files, :timeout])
    validate_request_opts!(opts)
    config = request_config(opts)

    with :ok <- validate_ref(ref),
         {:ok, current} <- Record.get(ref, Config.route_opts(config), config) do
      body(ref, current, config)
    end
  end

  defp body(_ref, nil, _config), do: {:ok, nil}

  defp body(ref, {version, %{body: {:inline, bytes}} = record}, _config),
    do: {:ok, {Record.to_file(ref, version, record), [bytes]}}

  defp body(ref, {version, %{body: {:object, key}} = record}, config) do
    case ObjectStore.download(config.objects, key, Config.object_opts(config)) do
      {:ok, {chunks, _size, _object_version}} ->
        {:ok, {Record.to_file(ref, version, record), chunks}}

      # Gone: the file was replaced or deleted too long ago.
      {:ok, nil} ->
        {:error, :unavailable}

      {:error, %Error{} = error} ->
        object_error(error)
    end
  end

  defp object_error(%Error{kind: :timeout}), do: {:error, :timeout}
  defp object_error(%Error{}), do: {:error, :unavailable}

  @doc """
  Lists a partition's files in id order, including their versions. Takes the
  `:prefix`, `:gte`, `:lt`, `:limit` and
  `:cursor` of `Slap.KV.scan/2`, and returns `{:ok, %{files: files, cursor:
  cursor}}`.
  """
  @spec list(binary(), keyword()) ::
          {:ok, %{files: [Slap.Files.File.t()], cursor: binary() | nil}} | {:error, error()}
  def list(partition, opts \\ []), do: span(:list, opts, fn -> do_list(partition, opts) end)

  defp do_list(partition, opts) do
    validate_options!(opts, [:files, :prefix, :gte, :lt, :limit, :cursor, :timeout])
    validate_request_opts!(opts)
    config = request_config(opts)
    scan_opts = Config.route_opts(config) ++ Keyword.drop(opts, [:files, :timeout])

    with :ok <- validate_partition(partition),
         {:ok, %{rows: rows, cursor: cursor}} <- Record.list(partition, scan_opts, config) do
      {:ok,
       %{
         files:
           for(
             {id, version, record} <- rows,
             do: Record.to_file({partition, id}, version, record)
           ),
         cursor: cursor
       }}
    end
  end

  defp span(operation, opts, fun) do
    files =
      if is_list(opts) and Keyword.keyword?(opts),
        do: Keyword.get(opts, :files, __MODULE__),
        else: __MODULE__

    metadata = %{files: files}

    :telemetry.span([:slap, :files, operation], metadata, fn ->
      result = fun.()
      outcome = if match?({:error, _}, result), do: :error, else: :ok
      {result, Map.put(metadata, :outcome, outcome)}
    end)
  end

  defp validate_ref({partition, id})
       when is_binary(partition) and partition != "" and is_binary(id) and id != "",
       do: :ok

  defp validate_ref(_ref), do: {:error, {:bad_request, :invalid_ref}}

  defp validate_partition(partition) when is_binary(partition) and partition != "", do: :ok
  defp validate_partition(_partition), do: {:error, {:bad_request, :invalid_partition}}

  defp validate_body(body) when is_binary(body), do: :ok

  defp validate_body(body) do
    if Enumerable.impl_for(body),
      do: :ok,
      else: {:error, {:bad_request, :invalid_body}}
  end

  defp request_config(opts) do
    config = Config.get(Keyword.get(opts, :files, __MODULE__))
    %{config | timeout: Keyword.get(opts, :timeout, config.timeout)}
  end

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp validate_request_opts!(opts) do
    validate_option!(
      opts,
      :storage,
      &(&1 in [:auto, :inline, :object]),
      ":auto, :inline or :object"
    )

    validate_option!(opts, :files, &module?/1, "an instance module")

    validate_option!(
      opts,
      :timeout,
      &valid_timeout?/1,
      "nil, a non-negative integer or :infinity"
    )
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp module?(value), do: is_atom(value) and value != nil

  defp valid_timeout?(value),
    do: value in [nil, :infinity] or (is_integer(value) and value >= 0)

  defp condition(opts, absent_allowed?) do
    case Keyword.fetch(opts, :if_version) do
      :error -> {:ok, :any}
      {:ok, nil} -> {:error, {:bad_request, :invalid_version}}
      {:ok, :absent} when absent_allowed? -> {:ok, :absent}
      {:ok, version} when is_integer(version) and version >= 0 -> {:ok, {:version, version}}
      _ -> {:error, {:bad_request, :invalid_version}}
    end
  end

  defp meta(opts) do
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")
    metadata = Keyword.get(opts, :metadata, %{})

    cond do
      not is_binary(content_type) ->
        {:error, {:bad_request, :invalid_content_type}}

      not (is_map(metadata) and
               Enum.all?(metadata, fn {k, v} -> is_binary(k) and is_binary(v) end)) ->
        {:error, {:bad_request, :invalid_metadata}}

      true ->
        {:ok, %{content_type: content_type, metadata: metadata}}
    end
  end

  defp verify(record, opts) do
    case Keyword.get(opts, :expected_sha256) do
      nil -> :ok
      expected when expected == record.sha256 -> :ok
      _ -> {:error, :checksum_mismatch}
    end
  end

  defp holds?(_current, :any), do: true
  defp holds?(nil, :absent), do: true
  defp holds?({version, _}, {:version, version}), do: true
  defp holds?(_current, _condition), do: false

  defp version_of(nil), do: nil
  defp version_of({version, _}), do: version

  defp if_version(nil), do: :absent
  defp if_version({version, _}), do: version
end
