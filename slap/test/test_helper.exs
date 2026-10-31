Code.require_file("../bench/support/raw_http.exs", __DIR__)
Code.require_file("support/inspect_peers.ex", __DIR__)
Code.require_file("support/storage_proxy.ex", __DIR__)

exclude = if System.get_env("SLAP_TEST_S3_ENDPOINT"), do: [], else: [:s3]
ExUnit.start(exclude: exclude)
Slap.SlateDB.set_log_level(:none)
