defmodule Slap.SlateDB.Admin do
  @moduledoc """
  Checkpoints, clones, garbage collection and other work on a database's
  files, without opening the database.

  An admin handle only reads and updates the manifest in object storage, so
  it can be used while a writer has the database open, here or elsewhere.

      {:ok, admin} = Slap.SlateDB.Admin.open("my-db", store: store)
      {:ok, %{id: id}} = Slap.SlateDB.Admin.create_checkpoint(admin, name: "nightly")
      {:ok, [%{id: ^id, name: "nightly"}]} = Slap.SlateDB.Admin.list_checkpoints(admin)
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Native, Options}

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @type checkpoint :: %{
          id: String.t(),
          manifest_id: non_neg_integer(),
          created_at: DateTime.t(),
          expires_at: DateTime.t() | nil,
          name: String.t() | nil
        }

  @doc """
  Opens an admin handle for the database at `path`. Takes the `:store` and
  `:timeout` options of `Slap.SlateDB.open/2`.
  """
  @spec open(String.t(), keyword()) :: {:ok, t()} | {:error, SlateDB.Error.t()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    Keyword.validate!(opts, [:store, :timeout])
    store = Options.store(Keyword.fetch!(opts, :store))
    open = &Native.admin_open(path, store, &1)

    with {:ok, resource} <- Native.call(open, Native.timeout(opts)) do
      {:ok, %__MODULE__{resource: resource}}
    end
  end

  @doc """
  Creates a checkpoint of the database's latest manifest. Unlike
  `Slap.SlateDB.create_checkpoint/2`, it does not need the database open, and it
  only covers what the writer has already written to the manifest.

  Takes the `:lifetime`, `:name`, `:source` and `:timeout` options of
  `Slap.SlateDB.create_checkpoint/2`.
  """
  @spec create_checkpoint(t(), keyword()) ::
          {:ok, %{id: String.t(), manifest_id: non_neg_integer()}}
          | {:error, SlateDB.Error.t()}
  def create_checkpoint(%__MODULE__{resource: admin}, opts \\ []) do
    Keyword.validate!(opts, [:lifetime, :source, :name, :timeout])
    lifetime = Keyword.get(opts, :lifetime)
    source = Keyword.get(opts, :source)
    name = Keyword.get(opts, :name)
    create = &Native.admin_create_checkpoint(admin, lifetime, source, name, &1)

    with {:ok, {id, manifest_id}} <- Native.call(create, Native.timeout(opts)) do
      {:ok, %{id: id, manifest_id: manifest_id}}
    end
  end

  @doc """
  Lists checkpoints, oldest first. Pass `name: name` to list only those with
  that name.
  """
  @spec list_checkpoints(t(), keyword()) ::
          {:ok, [checkpoint()]} | {:error, SlateDB.Error.t()}
  def list_checkpoints(%__MODULE__{resource: admin}, opts \\ []) do
    Keyword.validate!(opts, [:name, :timeout])
    list = &Native.admin_list_checkpoints(admin, Keyword.get(opts, :name), &1)

    with {:ok, rows} <- Native.call(list, Native.timeout(opts)) do
      {:ok,
       for {id, manifest_id, created, expires, name} <- rows do
         %{
           id: id,
           manifest_id: manifest_id,
           created_at: DateTime.from_unix!(created, :millisecond),
           expires_at: expires && DateTime.from_unix!(expires, :millisecond),
           name: name
         }
       end}
    end
  end

  @doc """
  Sets a checkpoint to expire `lifetime` milliseconds from now, or never when
  `lifetime` is `nil`.
  """
  @spec refresh_checkpoint(t(), String.t(), non_neg_integer() | nil, keyword()) ::
          :ok | {:error, SlateDB.Error.t()}
  def refresh_checkpoint(%__MODULE__{resource: admin}, id, lifetime, opts \\ [])
      when is_binary(id) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.admin_refresh_checkpoint(admin, id, lifetime, &1), Native.timeout(opts))
  end

  @doc "Deletes a checkpoint. Garbage collection can then remove its files."
  @spec delete_checkpoint(t(), String.t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def delete_checkpoint(%__MODULE__{resource: admin}, id, opts \\ []) when is_binary(id) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.admin_delete_checkpoint(admin, id, &1), Native.timeout(opts))
  end

  @doc """
  Runs the garbage collector once, deleting files that no manifest or
  checkpoint needs any more.

  ## Options

    * `:settings` - a map merged over SlateDB's default garbage collector
      options, for example
      `%{compacted_options: %{min_age: "1h"}, wal_options: %{min_age: "1h"}}`.
    * `:timeout` - see `Slap.SlateDB`.
  """
  @spec run_gc(t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def run_gc(%__MODULE__{resource: admin}, opts \\ []) do
    Keyword.validate!(opts, [:settings, :timeout])
    options_json = Options.settings(Keyword.get(opts, :settings))
    Native.call(&Native.admin_run_gc(admin, options_json, &1), Native.timeout(opts))
  end

  @doc """
  Creates the database at `clone_path` as a clone of this one, as of a
  checkpoint or of its latest state. The clone shares the source's files
  instead of copying them, and then changes independently.

  The clone is in the same store (and under the same URL prefix). Open it
  with `Slap.SlateDB.open/2`. For a `:memory` store this only works while the
  store is shared, which it is not across separate `open` calls, so use a
  local or object store.

  ## Options

    * `:checkpoint` - the checkpoint id to clone from. By default, SlateDB
      makes a checkpoint of the latest state.
    * `:timeout` - see `Slap.SlateDB`.
  """
  @spec clone(t(), String.t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def clone(%__MODULE__{resource: admin}, clone_path, opts \\ []) when is_binary(clone_path) do
    Keyword.validate!(opts, [:checkpoint, :timeout])
    checkpoint = Keyword.get(opts, :checkpoint)
    Native.call(&Native.admin_clone(admin, clone_path, checkpoint, &1), Native.timeout(opts))
  end

  @doc """
  Asks the database's compactor to merge all sorted runs into one.

  This rewrites every sorted run, which is how a `Slap.SlateDB.CompactionFilter`
  reaches data that was compacted before its prefix was added. It is the same
  plan as SlateDB's own full compaction: L0 SSTs are left to the normal
  schedule. A compactor must be running, for example in the process that has
  the database open.

  Returns `{:ok, compaction_id}`, or `{:ok, nil}` when there are no sorted
  runs yet. The compaction runs in the background; it may be rejected if it
  conflicts with a compaction already running.
  """
  @spec compact(t(), keyword()) :: {:ok, String.t() | nil} | {:error, SlateDB.Error.t()}
  def compact(%__MODULE__{resource: admin}, opts \\ []) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.admin_compact(admin, &1), Native.timeout(opts))
  end

  @doc """
  Returns when the write with sequence number `seq` happened, as recorded by
  SlateDB's sequence tracker, or `nil` if it has no record. The tracker
  keeps a sample of points, so the result is approximate. With
  `round_up: true`, it rounds to the next recorded point instead of the
  previous one.
  """
  @spec timestamp_for_seq(t(), non_neg_integer(), keyword()) ::
          {:ok, DateTime.t() | nil} | {:error, SlateDB.Error.t()}
  def timestamp_for_seq(%__MODULE__{resource: admin}, seq, opts \\ []) when is_integer(seq) do
    Keyword.validate!(opts, [:round_up, :timeout])
    round_up = Keyword.get(opts, :round_up, false)

    with {:ok, ms} <-
           Native.call(
             &Native.admin_timestamp_for_seq(admin, seq, round_up, &1),
             Native.timeout(opts)
           ) do
      {:ok, ms && DateTime.from_unix!(ms, :millisecond)}
    end
  end

  @doc """
  Returns the sequence number of the write at `datetime`, from SlateDB's
  sequence tracker, or `nil` if it has no record. Approximate, like
  `timestamp_for_seq/3`, and takes the same `:round_up` option.
  """
  @spec seq_for_timestamp(t(), DateTime.t(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, SlateDB.Error.t()}
  def seq_for_timestamp(%__MODULE__{resource: admin}, %DateTime{} = datetime, opts \\ []) do
    Keyword.validate!(opts, [:round_up, :timeout])
    round_up = Keyword.get(opts, :round_up, false)
    ms = DateTime.to_unix(datetime, :millisecond)
    Native.call(&Native.admin_seq_for_timestamp(admin, ms, round_up, &1), Native.timeout(opts))
  end
end
