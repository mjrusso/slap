exclude = if System.get_env("SLAP_TEST_S3_ENDPOINT"), do: [], else: [:s3]
ExUnit.start(exclude: exclude)

Slap.SlateDB.set_log_level(:none)
