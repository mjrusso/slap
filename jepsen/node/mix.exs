defmodule JepsenNode.MixProject do
  use Mix.Project

  def project do
    [
      app: :jepsen_node,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: [jepsen_node: [include_executables_for: [:unix]]]
    ]
  end

  def application do
    [mod: {JepsenNode.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [
      {:slap, path: "../../slap"},
      {:slap_yjs, path: "../../slap_yjs"},
      {:slap_files, path: "../../slap_files"},
      {:slap_snapshot_log, path: "../../slap_snapshot_log"},
      {:rustler, "~> 0.38", runtime: false}
    ]
  end
end
