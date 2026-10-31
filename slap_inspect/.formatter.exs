[
  plugins: [Breeze.HTMLFormatter],
  import_deps: [:breeze],
  inputs: [
    "{mix,.formatter,.credo,.ex_dna}.exs",
    "*.{ex,exs}",
    "{config,lib,test,scripts,bench}/**/*.{ex,exs}"
  ]
]
