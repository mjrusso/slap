defmodule Slap.SlateDB.Read do
  @moduledoc false
  # Point reads on any handle that can read: a `Slap.SlateDB`,
  # `Slap.SlateDB.Snapshot`, `Slap.SlateDB.Transaction` or
  # `Slap.SlateDB.Reader`. Each of those modules checks the handle's type
  # before calling this. Scans are in `Slap.SlateDB.Iterator`.

  alias Slap.SlateDB.{Native, Options}

  def get(%{resource: resource}, key, opts) when is_binary(key) do
    read_opts = Options.read(opts)
    Native.call(&Native.read_get(resource, key, read_opts, &1), Native.timeout(opts))
  end

  def get_key_value(%{resource: resource}, key, opts) when is_binary(key) do
    read_opts = Options.read(opts)
    get = &Native.read_get_key_value(resource, key, read_opts, &1)

    case Native.call(get, Native.timeout(opts)) do
      {:ok, {key, value, seq, create_ts, expire_ts}} ->
        {:ok, %{key: key, value: value, seq: seq, create_ts: create_ts, expire_ts: expire_ts}}

      other ->
        other
    end
  end
end
