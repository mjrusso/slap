defmodule Slap.Inspect.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mjrusso/slap"

  def project do
    [
      app: :slap_inspect,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      escript: [main_module: Slap.Inspect.CLI, name: "slap-inspect"],
      dialyzer: [plt_add_apps: [:mix, :ex_unit]],
      usage_rules: [file: "AGENTS.md", usage_rules: [:usage_rules]],
      description: "Read-only terminal UI and CLI for browsing Slap data.",
      source_url: @source_url,
      package: [
        licenses: ["Apache-2.0"],
        links: %{"GitHub" => @source_url},
        files: ~w(lib config mix.exs mix.lock README.md LICENSE CHANGELOG.md)
      ],
      docs: [
        main: "readme",
        extras: ["README.md", "CHANGELOG.md", "LICENSE"],
        search: [
          %{name: "slap_inspect", help: "Search slap_inspect"},
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
        source_url_pattern:
          "#{@source_url}/blob/slap_inspect-v#{@version}/slap_inspect/%{path}#L%{line}"
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
      {:back_breeze, "~> 0.4.5"},
      {:breeze, "== 0.5.0"},
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

  defp aliases do
    [
      check: [
        "compile --warnings-as-errors --force",
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo",
        "reach.check --arch",
        "dialyzer --format short",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "test"
      ]
    ]
  end
end
