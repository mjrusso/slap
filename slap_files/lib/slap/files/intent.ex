defmodule Slap.Files.Intent do
  @moduledoc false
  # An intent names objects of a file (`ref`) that may have to be deleted:
  # a body being uploaded, or the old body of a replaced or deleted file.
  # It is written before those objects become unreferenced (or exist), and
  # deleted once they are dealt with, so every object that is not a file's
  # body is named by an intent (see Slap.Files.Sweeper).
  #
  # Intents are Slap.KV rows in 64 namespaced partitions (by a hash of the
  # ref), keyed by a random id. A writer and the sweeper each
  # take an intent with a conditional rewrite before acting on it, so at
  # most one of them acts on its objects: a writer before it points a file
  # at the new body, the sweeper before it deletes objects.
  #
  # An intent is not opened once it is due (a Slap.KV deadline): an open
  # that its writer gave up on cannot create it after the sweeps that
  # would have found it. A rewrite is conditional, so it cannot recreate
  # an intent that is gone.

  alias Slap.Files.{Config, Deadline, Scan}
  alias Slap.KV

  @buckets 64

  @enforce_keys [:bucket, :id, :ref, :keys, :due_ms]
  defstruct [:bucket, :id, :version, :ref, :keys, :due_ms, :config, taken: nil]

  @type t :: %__MODULE__{
          bucket: non_neg_integer(),
          id: binary(),
          version: KV.version() | nil,
          ref: Slap.Files.ref(),
          keys: [String.t()],
          due_ms: integer(),
          config: Config.t(),
          taken: nil | :writer | :sweeper
        }

  @spec buckets() :: Range.t()
  def buckets, do: 0..(@buckets - 1)

  @spec bucket(Slap.Files.ref()) :: non_neg_integer()
  def bucket(ref), do: :erlang.phash2(ref, @buckets)

  @spec partition(non_neg_integer(), Config.t()) :: binary()
  def partition(bucket, config), do: Config.partition(config, :intent, bucket)

  @spec open(Slap.Files.ref(), [String.t()], integer(), Config.t()) ::
          {:ok, t()} | {:error, term()}
  def open(ref, keys, due_ms, config) do
    intent = %__MODULE__{
      bucket: bucket(ref),
      id: :crypto.strong_rand_bytes(16),
      ref: ref,
      keys: keys,
      due_ms: due_ms,
      config: config
    }

    with {:ok, version} <- write(intent, :absent, Deadline.opts(due_ms, config)),
         do: {:ok, %{intent | version: version}}
  end

  @spec take(t(), keyword()) :: {:ok, t()} | {:error, :taken | term()}
  def take(intent, changes) do
    new = struct!(intent, changes)

    case write(new, intent.version) do
      {:ok, version} -> {:ok, %{new | version: version}}
      {:error, {:conflict, _}} -> {:error, :taken}
      {:error, _} = error -> error
    end
  end

  @spec done(t()) :: :ok | {:error, term()}
  def done(intent) do
    case KV.delete(
           partition(intent.bucket, intent.config),
           intent.id,
           [if_version: intent.version] ++ Config.route_opts(intent.config)
         ) do
      {:error, {:conflict, _}} -> :ok
      other -> other
    end
  end

  @spec get(non_neg_integer(), binary(), Config.t()) :: {:ok, t() | nil} | {:error, term()}
  def get(bucket, id, config) do
    case KV.get(partition(bucket, config), id, Config.route_opts(config)) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, %{value: value, version: version}} ->
        {:ok, decode(bucket, id, value, version, config)}

      {:error, _} = error ->
        error
    end
  end

  @spec reduce(non_neg_integer(), acc, (t(), acc -> acc), Config.t()) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def reduce(bucket, acc, fun, config) do
    Scan.reduce(
      partition(bucket, config),
      acc,
      fn {id, value}, acc ->
        fun.(decode(bucket, id, value, nil, config), acc)
      end,
      Config.route_opts(config)
    )
  end

  defp write(intent, if_version, opts \\ []) do
    value = {:intent, 1, Map.take(intent, [:ref, :keys, :due_ms, :taken])}
    opts = [if_version: if_version] ++ Config.route_opts(intent.config) ++ opts

    Deadline.result(
      KV.put(
        partition(intent.bucket, intent.config),
        intent.id,
        :erlang.term_to_binary(value),
        opts
      )
    )
  end

  defp decode(bucket, id, value, version, config) do
    {:intent, 1, fields} = :erlang.binary_to_term(value, [:safe])

    struct!(
      %__MODULE__{
        bucket: bucket,
        id: id,
        version: version,
        ref: nil,
        keys: [],
        due_ms: 0,
        config: config
      },
      fields
    )
  end
end
