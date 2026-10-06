defmodule Slap.SlateDB.Transaction do
  @moduledoc """
  A read-write transaction.

  Start one with `Slap.SlateDB.begin/2` or, more simply, `Slap.SlateDB.transaction/3`.
  Reads see the database as of the start of the transaction plus the
  transaction's own writes. Writes stay in memory until `commit/2`.

  A transaction belongs to the process that uses it. Do not use it from more
  than one process at the same time.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Iterator, Native, Read}

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @doc """
  Reads `key`, including this transaction's uncommitted writes. Takes the
  options of `Slap.SlateDB.get/3`.
  """
  @spec get(t(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, SlateDB.Error.t()}
  # Typed entry points to the shared read path (`Slap.SlateDB.Read` and
  # `Slap.SlateDB.Iterator`). Each handle module has the same ones, so the
  # duplication checker is told to skip them.
  # ex_dna:disable-for-next-line
  def get(%__MODULE__{} = tx, key, opts \\ []), do: Read.get(tx, key, opts)

  @doc """
  Reads `key` with its metadata, like `Slap.SlateDB.get_key_value/3`,
  including this transaction's uncommitted writes. Their `:seq` is nil until
  commit.
  """
  @spec get_key_value(t(), binary(), keyword()) ::
          {:ok, SlateDB.key_value() | nil} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def get_key_value(%__MODULE__{} = tx, key, opts \\ []), do: Read.get_key_value(tx, key, opts)

  @doc """
  Scans rows, including this transaction's uncommitted writes. With
  `with_versions: true`, those writes have a nil version. See
  `Slap.SlateDB.scan/2`.
  """
  @spec scan(t(), keyword()) :: Enumerable.t()
  # ex_dna:disable-for-next-line
  def scan(%__MODULE__{} = tx, opts \\ []), do: Iterator.stream(tx, opts)

  @doc "Opens an iterator over the transaction's rows. See `Slap.SlateDB.iterator/2`."
  @spec iterator(t(), keyword()) :: {:ok, Iterator.t()} | {:error, SlateDB.Error.t()}
  def iterator(%__MODULE__{} = tx, opts \\ []), do: Iterator.open(tx, opts)

  @doc """
  Adds a put to the transaction.

  ## Options

    * `:ttl` - time to live in milliseconds. By default, the database's
      `default_ttl_millis` setting applies.
  """
  @spec put(t(), binary(), binary(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def put(%__MODULE__{resource: tx}, key, value, opts \\ [])
      when is_binary(key) and is_binary(value) do
    Keyword.validate!(opts, [:ttl])
    Native.normalize(Native.tx_put(tx, key, value, Keyword.get(opts, :ttl)))
  end

  @doc """
  Adds a merge operand to the transaction. The database must be opened with a
  `:merge_operator`; see `Slap.SlateDB.merge/4`. Takes the `:ttl` option of `put/4`.
  """
  @spec merge(t(), binary(), binary(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def merge(%__MODULE__{resource: tx}, key, operand, opts \\ [])
      when is_binary(key) and is_binary(operand) do
    Keyword.validate!(opts, [:ttl])
    Native.normalize(Native.tx_merge(tx, key, operand, Keyword.get(opts, :ttl)))
  end

  @doc "Adds a delete to the transaction."
  @spec delete(t(), binary()) :: :ok | {:error, SlateDB.Error.t()}
  def delete(%__MODULE__{resource: tx}, key) when is_binary(key) do
    Native.normalize(Native.tx_delete(tx, key))
  end

  @doc """
  Commits the transaction.

  Returns `{:ok, seq}` with the commit's sequence number, or `{:ok, nil}` if
  the transaction made no writes. Returns
  `{:error, %Slap.SlateDB.Error{kind: :conflict}}` if the transaction conflicts
  with another one that committed first.

  ## Options

    * `:await_durable` - when `true`, returns only after the writes are durable
      in object storage. Defaults to `false`.
    * `:timeout` - see `Slap.SlateDB.get/3`.
  """
  @spec commit(t(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, SlateDB.Error.t()}
  def commit(%__MODULE__{resource: tx}, opts \\ []) do
    opts = Keyword.validate!(opts, [:await_durable, :timeout])
    await_durable = Keyword.get(opts, :await_durable, false)
    Native.call(&Native.tx_commit(tx, await_durable, &1), Native.timeout(opts))
  end

  @doc "Throws away the transaction's writes. Rolling back twice is allowed."
  @spec rollback(t()) :: :ok
  def rollback(%__MODULE__{resource: tx}) do
    Native.call(&Native.tx_rollback(tx, &1))
  end
end
