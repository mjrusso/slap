import Config

config :logger, level: :warning
config :logger, :default_handler, config: [type: :standard_error]
