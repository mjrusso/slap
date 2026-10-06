exclude = if System.get_env("SLAP_TEST_S3_ENDPOINT"), do: [], else: [:s3]
ExUnit.start(exclude: exclude, capture_log: true)
