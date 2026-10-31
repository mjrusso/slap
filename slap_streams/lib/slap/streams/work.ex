defmodule Slap.Streams.Work do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.Streams.Store.Keys

  def each_local(fun) do
    Registry.select(__MODULE__.Registry, [{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Task.async_stream(&entries(&1, fun), max_concurrency: 8, timeout: :infinity)
    |> Stream.run()
  end

  defp entries({{cluster, _shard}, ctx}, fun) do
    ctx.db
    |> SlateDB.scan(prefix: Keys.prefix(:work), durability: :remote)
    |> Enum.each(fn {_key, path} -> fun.({cluster, path}) end)
  rescue
    SlateDB.Error -> :ok
  end
end
