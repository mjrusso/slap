defmodule Slap.Yjs.Test.ClusterCase do
  @moduledoc false

  alias Slap.Streams
  alias Slap.Yjs
  # Starts Slap.Streams.Cluster for each test: on a fresh local directory, or on RustFS
  # with `@tag :s3` (SLAP_TEST_S3_ENDPOINT, SLAP_TEST_S3_BUCKET).

  use ExUnit.CaseTemplate

  using do
    quote do
      import Slap.Yjs.Test.ClusterCase
    end
  end

  setup context do
    store = if context[:s3], do: s3_store(), else: local_store()
    settings = Map.merge(%{flush_interval: "2ms"}, context[:settings] || %{})
    opts = [store: store, shards: 2, settings: settings]
    start_supervised!({Streams.Cluster, opts})
    start_supervised!(Yjs.Docs)
    %{cluster_opts: opts}
  end

  defp local_store do
    dir = Path.join(System.tmp_dir!(), "yjs-test-#{System.unique_integer([:positive])}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)
    {:local, dir}
  end

  defp s3_store do
    endpoint = System.fetch_env!("SLAP_TEST_S3_ENDPOINT")
    bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")

    {:url, "s3://#{bucket}/yjs-store/test-#{System.unique_integer([:positive])}",
     aws_endpoint: endpoint,
     aws_allow_http: "true",
     aws_region: "us-east-1",
     aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
     aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")}
  end

  @doc "A document id unique to the test."
  def doc_id, do: {"test", "doc-#{System.unique_integer([:positive])}"}
end
