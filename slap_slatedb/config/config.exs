import Config

# Slap.SlateDB.LogForwarder tags SlateDB's log records with the Rust module they
# come from, as `slatedb_target` metadata. Applications add the key to their
# own formatter to see it; this config applies to this project's tests only.
config :logger, :default_formatter, metadata: [:slatedb_target]
