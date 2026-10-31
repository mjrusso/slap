defmodule Mix.Tasks.Slap.Inspect do
  @shortdoc "Inspects durable data on a running Slap node"
  @moduledoc """
  Runs the inspector from a source checkout. Use `mix slap.inspect --help`
  for connection options and commands.
  """
  use Mix.Task

  alias Slap.Inspect.CLI

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    CLI.main(args)
  end
end
