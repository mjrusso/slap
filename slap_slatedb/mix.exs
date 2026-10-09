defmodule Slap.SlateDB.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/mjrusso/slap"

  def project do
    [
      app: :slap_slatedb,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      usage_rules: [file: "AGENTS.md", usage_rules: [:usage_rules]],
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      description: "Elixir bindings for SlateDB, a key-value store built on object storage.",
      package: package(),
      source_url: @source_url,
      docs: [
        main: "readme",
        extras: ["README.md", "CHANGELOG.md", "LICENSE"],
        groups_for_modules: [
          Database: [
            Slap.SlateDB,
            Slap.SlateDB.Transaction,
            Slap.SlateDB.Snapshot,
            Slap.SlateDB.Reader,
            Slap.SlateDB.Iterator
          ],
          Storage: [
            Slap.SlateDB.ObjectStore,
            Slap.SlateDB.Cache,
            Slap.SlateDB.Admin,
            Slap.SlateDB.MergeOperator,
            Slap.SlateDB.CompactionFilter
          ],
          Support: [Slap.SlateDB.Error, Slap.SlateDB.Subscription, Slap.SlateDB.Telemetry]
        ],
        source_url_pattern:
          "#{@source_url}/blob/slap_slatedb-v#{@version}/slap_slatedb/%{path}#L%{line}"
      ]
    ]
  end

  def application do
    [
      mod: {Slap.SlateDB.Application, []},
      extra_applications: [:logger],
      env: [log_level: :warning]
    ]
  end

  def cli do
    [preferred_envs: [check: :test]]
  end

  defp deps do
    [
      {:rustler_precompiled, "~> 0.9"},
      {:rustler, "~> 0.38", optional: true},
      {:telemetry, "~> 1.0"},
      {:benchee, "~> 1.5", only: :bench},
      {:benchee_html, "~> 1.0", only: :bench},
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

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url, "SlateDB" => "https://slatedb.io"},
      files: [
        "lib",
        "native/slatedb_nif/src",
        "native/slatedb_nif/.cargo",
        "native/slatedb_nif/Cargo.toml",
        "native/slatedb_nif/Cargo.lock",
        # Written by `mix rustler_precompiled.download Slap.SlateDB.Native --all`
        # when releasing. See RELEASING.md at the repository root.
        "checksum-*.exs",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "LICENSE"
      ]
    ]
  end

  defp aliases do
    [
      check: [
        "compile --warnings-as-errors --force",
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo",
        "dialyzer --format short",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "test"
      ]
    ]
  end
end
