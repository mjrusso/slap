defmodule Slap.KV.MixProject do
  use Mix.Project

  @version "0.6.1"
  @source_url "https://github.com/mjrusso/slap"
  @package_url "#{@source_url}/tree/main/slap_kv"

  def project do
    [
      app: :slap_kv,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:ex_unit]],
      usage_rules: [file: "AGENTS.md", usage_rules: [:usage_rules]],
      description: "A partitioned key-value store that persists data using SlateDB.",
      source_url: @package_url,
      package: [
        licenses: ["Apache-2.0"],
        links: %{"GitHub (Package)" => @package_url, "GitHub (Monorepo)" => @source_url}
      ],
      docs: [
        main: "readme",
        extras: ["README.md", "CHANGELOG.md", "LICENSE"],
        search: [
          %{name: "slap_kv", help: "Search slap_kv"},
          %{
            name: "All Slap packages",
            help: "Search every Slap package on HexDocs",
            packages: [
              :slap_slatedb,
              :slap_cluster,
              :slap_streams,
              :slap_snapshot_log,
              :slap_yjs,
              :slap_kv,
              :slap_files,
              :slap_inspect,
              :slap
            ]
          }
        ],
        groups_for_modules: [
          KV: [Slap.KV, Slap.KV.Cluster],
          HTTP: [Slap.KV.HTTP.Router],
          Metrics: [Slap.KV.Telemetry]
        ],
        source_url_pattern: "#{@source_url}/blob/slap_kv-v#{@version}/slap_kv/%{path}#L%{line}"
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
      slap_dep(:slap_cluster, "~> 0.6.1"),
      slap_dep(:slap_slatedb, "~> 0.4.0"),
      {:plug, "~> 1.20"},
      {:telemetry, "~> 1.0"},
      {:rustler, "~> 0.38", optional: true, runtime: false},
      {:stream_data, "~> 1.4", only: [:dev, :test]},
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
