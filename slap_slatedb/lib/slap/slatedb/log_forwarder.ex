defmodule Slap.SlateDB.LogForwarder do
  @moduledoc false

  alias Slap.SlateDB
  # Receives SlateDB's log records from the NIF and passes them to `Logger`.
  #
  # The NIF queues records on a bounded channel and drops them when it is
  # full, so SlateDB never waits on Elixir. `dropped` counts records lost
  # since the last message.
  use GenServer
  require Logger

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    level = Application.get_env(:slap_slatedb, :log_level, :warning)
    :ok = SlateDB.Native.log_init(self(), SlateDB.Options.log_level(level))
    {:ok, nil}
  end

  @impl true
  def handle_info({:slap_slatedb_log, level, target, message, dropped}, state) do
    if dropped > 0 do
      Logger.warning("SlateDB dropped #{dropped} log messages because Logger fell behind",
        slatedb_target: "slap_slatedb"
      )
    end

    Logger.log(level, message, slatedb_target: target)
    {:noreply, state}
  end
end
