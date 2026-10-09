defmodule Slap.SlateDB.Native do
  @moduledoc false

  alias Slap.SlateDB
  # NIF stubs, plus the helper that waits for async NIF replies.
  #
  # Most NIFs take a reference as their last argument, start the work on the
  # native Tokio runtime and return `:ok` at once. The result arrives later
  # as a `{:slap_slatedb_reply, ref, result}` message. A NIF that fails before it
  # starts (a bad argument, for example) returns `{:error, _}` directly.

  version = Mix.Project.config()[:version]

  # Prebuilt NIFs are attached to the GitHub release for this version (see
  # RELEASING.md at the repository root), and checked against the checksum
  # file.
  # SLAP_SLATEDB_BUILD=1 builds the NIF from source with rustler instead. The
  # targets must match the release workflow's build matrix.
  use RustlerPrecompiled,
    otp_app: :slap_slatedb,
    crate: "slatedb_nif",
    base_url: "https://github.com/mjrusso/slap/releases/download/slap_slatedb-v#{version}",
    version: version,
    force_build: System.get_env("SLAP_SLATEDB_BUILD") in ["1", "true"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
    ),
    nif_versions: ["2.15"]

  @doc false
  # A timed-out write is not cancelled; ordered writers leave this infinite.
  def timeout(opts) do
    case Keyword.get(opts, :timeout, :infinity) do
      :infinity ->
        :infinity

      ms when is_integer(ms) and ms >= 0 ->
        ms

      other ->
        raise ArgumentError, "expected :timeout to be :infinity or ms, got: #{inspect(other)}"
    end
  end

  @doc false
  # Calls `fun` with a new reference and waits for the reply.
  def call(fun, timeout \\ :infinity)

  # `make_ref/0` and `receive` are in the same function, so the compiler can
  # skip older messages in the mailbox instead of scanning all of them.
  def call(fun, :infinity) do
    ref = make_ref()

    case fun.(ref) do
      :ok ->
        receive do
          {:slap_slatedb_reply, ^ref, result} -> normalize(result)
        end

      other ->
        normalize(other)
    end
  end

  # The NIF replies to the process that called it, and a reply that comes
  # after the timeout must not land in the caller's mailbox. So a short-lived
  # worker makes the call and forwards the result through an alias. On
  # timeout the alias is removed, so a late forward is dropped, and the worker
  # is killed. The operation itself is not cancelled.
  def call(fun, timeout) when is_integer(timeout) and timeout >= 0 do
    reply_to = Process.alias([:reply])

    {pid, monitor} =
      spawn_monitor(fn -> send(reply_to, {reply_to, call(fun, :infinity)}) end)

    receive do
      {^reply_to, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        Process.unalias(reply_to)

        case reason do
          # The worker raised, for example on a bad argument. Raise it here.
          {error, stacktrace} when is_list(stacktrace) -> :erlang.raise(:error, error, stacktrace)
          other -> exit(other)
        end
    after
      timeout ->
        Process.unalias(reply_to)
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])

        # A reply sent just before the alias was removed may already be here.
        receive do
          {^reply_to, _} -> :ok
        after
          0 -> :ok
        end

        {:error,
         %SlateDB.Error{
           kind: :timeout,
           reason: nil,
           message:
             "no reply within #{timeout} ms; the operation was not cancelled " <>
               "and may still complete"
         }}
    end
  end

  @doc false
  def normalize({:error, {kind, reason, message}}) do
    {:error, %SlateDB.Error{kind: kind, reason: reason, message: message}}
  end

  def normalize(result), do: result

  defp err, do: :erlang.nif_error(:nif_not_loaded)

  def runtime_init(_threads), do: err()
  def db_open(_path, _store, _settings_json, _cache, _merge_operator, _filter, _ref), do: err()
  def db_validate_settings(_settings_json), do: err()
  def db_close(_db, _ref), do: err()
  def db_flush(_db, _type, _ref), do: err()
  def db_merge(_db, _key, _operand, _ttl_ms, _await_durable, _ref), do: err()
  def db_create_checkpoint(_db, _scope, _lifetime, _source, _name, _ref), do: err()
  def db_put(_db, _key, _value, _ttl_ms, _await_durable, _ref), do: err()
  def db_delete(_db, _key, _await_durable, _ref), do: err()
  def db_write(_db, _ops, _await_durable, _ref), do: err()
  def db_write_dirty(_db, _ops, _await_durable, _ref), do: err()
  def db_snapshot(_db, _ref), do: err()
  def db_begin(_db, _isolation, _ref), do: err()
  def db_durable_seq(_db), do: err()
  def db_stats(_db), do: err()
  def db_cache_stats(_db), do: err()
  def db_metrics(_db), do: err()
  def db_subscribe(_db, _pid, _ref, _tag), do: err()
  def subscription_cancel(_sub), do: err()

  def cache_new(_capacity_bytes), do: err()
  def store_probe(_store, _path, _ref), do: err()
  def objstore_open(_store, _path, _ref), do: err()
  def objstore_get(_res, _key, _ref), do: err()
  def objstore_put(_res, _key, _body, _mode, _ref), do: err()
  def objstore_delete(_res, _key, _ref), do: err()
  def objstore_list(_res, _prefix, _ref), do: err()
  def objstore_upload_open(_res, _key, _ref), do: err()
  def objstore_upload_write(_upload, _chunk, _ref), do: err()
  def objstore_upload_finish(_upload, _ref), do: err()
  def objstore_upload_abort(_upload, _ref), do: err()
  def objstore_download_open(_res, _key, _range, _ref), do: err()
  def objstore_download_next(_download, _ref), do: err()

  def log_init(_pid, _level), do: err()
  def log_set_level(_level), do: err()

  # `target` is a database, snapshot, transaction or reader resource.
  def read_get(_target, _key, _opts, _ref), do: err()
  def read_get_key_value(_target, _key, _opts, _ref), do: err()
  def read_scan(_target, _range, _prefix, _opts, _ref), do: err()

  def tx_put(_tx, _key, _value, _ttl_ms), do: err()
  def tx_delete(_tx, _key), do: err()
  def tx_merge(_tx, _key, _operand, _ttl_ms), do: err()
  def tx_commit(_tx, _await_durable, _ref), do: err()
  def tx_rollback(_tx, _ref), do: err()

  def prefix_filter_new(_prefixes), do: err()
  def prefix_filter_update(_filter, _add, _remove), do: err()
  def prefix_filter_info(_filter), do: err()

  def reader_open(_path, _store, _options, _mode, _checkpoint, _cache, _merge_operator, _ref),
    do: err()

  def reader_close(_reader, _ref), do: err()
  def reader_durable_seq(_reader), do: err()

  def admin_open(_path, _store, _ref), do: err()
  def admin_create_checkpoint(_admin, _lifetime, _source, _name, _ref), do: err()
  def admin_list_checkpoints(_admin, _name, _ref), do: err()
  def admin_refresh_checkpoint(_admin, _id, _lifetime, _ref), do: err()
  def admin_delete_checkpoint(_admin, _id, _ref), do: err()
  def admin_run_gc(_admin, _options, _ref), do: err()
  def admin_clone(_admin, _clone_path, _checkpoint, _ref), do: err()
  def admin_compact(_admin, _ref), do: err()
  def admin_timestamp_for_seq(_admin, _seq, _round_up, _ref), do: err()
  def admin_seq_for_timestamp(_admin, _unix_ms, _round_up, _ref), do: err()

  def iterator_next_batch(_iter, _max, _with_versions, _ref), do: err()
  def iterator_seek(_iter, _key, _ref), do: err()
end
