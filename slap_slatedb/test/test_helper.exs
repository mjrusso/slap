# `:slow` tests take ~15 seconds; run them with `mix test --include slow`.
# `:s3` tests need an S3-compatible server; they run when
# SLAP_TEST_S3_ENDPOINT is set (see the README).
# `:azure` tests need Azurite; they run when SLAP_TEST_AZURITE is set.
# `:crash` tests kill a writer VM (about 10 seconds); run them with
# `mix test --include crash`.
exclude =
  [:slow, :crash] ++
    if(System.get_env("SLAP_TEST_S3_ENDPOINT"), do: [], else: [:s3]) ++
    if(System.get_env("SLAP_TEST_AZURITE"), do: [], else: [:azure])

ExUnit.start(exclude: exclude)

# SlateDB's own log records (fencing errors, for example) are off in tests.
# Set SLAP_TEST_LOG_LEVEL=warning (or debug, info, error) to see them.
Slap.SlateDB.set_log_level(String.to_existing_atom(System.get_env("SLAP_TEST_LOG_LEVEL", "none")))
