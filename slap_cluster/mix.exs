defmodule Slap.Cluster.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mjrusso/slap"

  def project do
    [
      app: :slap_cluster,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:ex_unit]],
      usage_rules: [file: "AGENTS.md", usage_rules: [:usage_rules]],
      description:
        "Runs N SlateDB databases (shards) in one store, with placement, fencing, " <>
          "durability notifications and routing.",
      source_url: @source_url,
      package: [licenses: ["Apache-2.0"], links: %{"GitHub" => @source_url}],
      docs: [
        main: "readme",
        extras: ["README.md", "CHANGELOG.md", "LICENSE"],
        groups_for_modules: [
          Cluster: [
            Slap.Cluster,
            Slap.Cluster.Shard,
            Slap.Cluster.ShardChildren,
            Slap.Cluster.Peers,
            Slap.Cluster.Hash
          ],
          Placement: [
            Slap.Cluster.Strategy,
            Slap.Cluster.Strategy.Local,
            Slap.Cluster.Strategy.Static,
            Slap.Cluster.Strategy.Distributed,
            Slap.Cluster.Strategy.ObjectLease,
            Slap.Cluster.Strategy.LeaseLoop,
            Slap.Cluster.Strategy.LeaseLoop.Store
          ]
        ],
        source_url_pattern:
          "#{@source_url}/blob/slap_cluster-v#{@version}/slap_cluster/%{path}#L%{line}"
      ]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [check: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      slap_dep(:slap_slatedb, "~> 0.2.0"),
      {:telemetry, "~> 1.0"},
      {:rustler, "~> 0.38", optional: true, runtime: false},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.5", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5.4", only: [:dev, :test], runtime: false},
      {:reach, "~> 2.8.4", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.8", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:usage_rules, "~> 1.2.8", only: :dev},
      {:igniter, "~> 0.8.4", only: :dev}
    ]
  end

  defp slap_dep(app, requirement) do
    case System.get_env("SLAP_LOCAL_DEPS") do
      "1" -> {app, requirement, path: "../#{app}"}
      _ -> {app, requirement}
    end
  end

  defp aliases do
    [
      check: [
        "compile --warnings-as-errors --force",
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo",
        "reach.check --arch",
        # The PLT's up-to-date check hashes only the lockfile, which path
        # dependencies are not in: --force-check updates their changed modules.
        "dialyzer --format short --force-check",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "test"
      ]
    ]
  end
end
