[
  # Root files are listed by name. A `*.exs` glob would also match
  # checksum-*.exs, which `mix rustler_precompiled.download` rewrites,
  # unformatted, on each release.
  inputs: [
    "{mix,.formatter,.credo,.ex_dna}.exs",
    "{config,lib,test,bench}/**/*.{ex,exs}"
  ]
]
