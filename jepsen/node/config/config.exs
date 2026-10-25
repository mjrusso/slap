import Config

if config_env() == :prod do
  config :slap_streams, Slap.Streams.Cluster, probe: :background
  config :slap_kv, Slap.KV.Cluster, probe: :background
end
