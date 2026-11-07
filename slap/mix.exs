defmodule Slap.MixProject do
  use Mix.Project

  @version "0.6.0"
  @source_url "https://github.com/mjrusso/slap"
  @package_url "#{@source_url}/tree/main/slap"
  @guides [
    "guides/overview.md",
    "guides/kitchen-sink.md",
    "guides/library-examples.md",
    "guides/standalone-server.md",
    "guides/development.md"
  ]

  def project do
    [
      app: :slap,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:mix, :iex]],
      usage_rules: [file: "AGENTS.md", usage_rules: [:usage_rules]],
      description: "All Slap server libraries, started together under one supervisor.",
      source_url: @package_url,
      package: [
        licenses: ["Apache-2.0"],
        links: %{"GitHub (Package)" => @package_url, "GitHub (Monorepo)" => @source_url},
        files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md) ++ @guides
      ],
      docs: [
        main: "readme",
        extras: ["README.md"] ++ @guides ++ ["CHANGELOG.md", "LICENSE"],
        filter_modules: fn _module, meta ->
          not String.starts_with?(
            List.to_string(meta.source_path),
            Path.expand("dev", __DIR__) <> "/"
          )
        end,
        search: [
          %{name: "slap", help: "Search slap"},
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
        source_url_pattern: "#{@source_url}/blob/slap-v#{@version}/slap/%{path}#L%{line}"
      ]
    ]
  end

  def application do
    [extra_applications: [:logger]] ++ dev_application(Mix.env())
  end

  # mix slap.server starts its server under this application, so that the
  # VM stops the server before the slap_* applications it runs on.
  defp dev_application(env) when env in [:dev, :test], do: [mod: {Slap.Dev.Application, []}]
  defp dev_application(_env), do: []

  defp elixirc_paths(env) when env in [:dev, :test], do: ["lib", "dev"]
  defp elixirc_paths(_env), do: ["lib"]

  def cli do
    [preferred_envs: [check: :test]]
  end

  defp deps do
    [
      slap_dep(:slap_streams, "~> 0.6.0"),
      slap_dep(:slap_snapshot_log, "~> 0.6.0"),
      slap_dep(:slap_yjs, "~> 0.6.0"),
      slap_dep(:slap_kv, "~> 0.6.0"),
      slap_dep(:slap_files, "~> 0.6.0"),
      slap_dep(:slap_cluster, "~> 0.6.0"),
      slap_dep(:slap_slatedb, "~> 0.4.0"),
      {:slap_inspect, path: "../slap_inspect", only: :test, runtime: false},
      {:bandit, "~> 1.12", only: [:dev, :test]},
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
