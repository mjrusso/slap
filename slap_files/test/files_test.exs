defmodule Slap.FilesTest do
  use Slap.Files.Test.FilesCase, async: false

  alias Slap.Files.Config
  alias Slap.Files.File, as: FileInfo
  alias Slap.Files.Sweeper

  defmodule OtherCluster do
    use Slap.KV.Cluster, otp_app: :slap_files
  end

  defmodule OtherFiles do
    @moduledoc false
  end

  test "file operations emit spans with their outcomes" do
    id = {__MODULE__, make_ref()}
    parent = self()

    :ok =
      :telemetry.attach_many(
        id,
        [
          [:slap, :files, :put, :start],
          [:slap, :files, :put, :stop],
          [:slap, :files, :put, :exception],
          [:slap, :files, :get, :stop]
        ],
        &__MODULE__.send_telemetry/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(id) end)

    assert {:ok, _} = Files.put({"telemetry", "file"}, "body")

    assert_receive {[:slap, :files, :put, :start], %{system_time: _},
                    %{telemetry_span_context: context, files: Files}}

    assert_receive {[:slap, :files, :put, :stop], %{duration: duration},
                    %{outcome: :ok, telemetry_span_context: ^context, files: Files}}

    assert duration >= 0

    assert {:error, {:bad_request, :invalid_ref}} = Files.get({"", "file"})
    assert_receive {[:slap, :files, :get, :stop], _, %{outcome: :error, files: Files}}

    start_supervised!(%{
      id: OtherFiles,
      start: {Files, :start_link, [[store: :memory, name: OtherFiles, namespace: "telemetry"]]}
    })

    assert {:ok, nil} = Files.get({"telemetry", "other"}, files: OtherFiles)
    assert_receive {[:slap, :files, :get, :stop], _, %{outcome: :ok, files: OtherFiles}}

    assert_raise ArgumentError, fn -> Files.put({"telemetry", "bad"}, "body", storage: :bad) end

    assert_receive {[:slap, :files, :put, :exception], %{duration: _},
                    %{kind: :error, reason: %ArgumentError{}}}
  end

  def send_telemetry(event, measurements, metadata, parent),
    do: send(parent, {event, measurements, metadata})

  test "a second Files instance isolates records, bodies and its sweeper" do
    dir = Path.join(System.tmp_dir!(), "files-other-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    start_supervised!({OtherCluster, store: :memory, shards: 1})

    start_supervised!(%{
      id: OtherFiles,
      start:
        {Files, :start_link,
         [
           [
             name: OtherFiles,
             cluster: OtherCluster,
             namespace: "other",
             store: {:local, dir},
             inline_max_bytes: 1
           ]
         ]}
    })

    ref = {"other", "file"}
    assert {:ok, %FileInfo{storage: :object}} = Files.put(ref, "object body", files: OtherFiles)
    assert {:ok, "object body"} = Files.read(ref, files: OtherFiles)
    assert {:ok, nil} = Files.get(ref)
    assert {:ok, %{files: [%FileInfo{ref: ^ref}]}} = Files.list("other", files: OtherFiles)
    assert :ok = Files.delete(ref, files: OtherFiles)
    assert :ok = Sweeper.sweep(OtherFiles)
  end

  test "a named instance keeps its namespace across a rename and removes config on stop" do
    dir = Path.join(System.tmp_dir!(), "files-named-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    assert_raise ArgumentError, ~r/namespace is required/, fn ->
      Config.new(Config.get().objects, name: OtherFiles)
    end

    start_supervised!(%{
      id: OtherFiles,
      start: {Files, :start_link, [[name: OtherFiles, namespace: "stable", store: {:local, dir}]]}
    })

    ref = {"named", "file"}
    assert {:ok, _} = Files.put(ref, "body", files: OtherFiles)
    assert {:ok, nil} = Files.get(ref)
    assert {:ok, "body"} = Files.read(ref, files: OtherFiles)

    stop_supervised!(OtherFiles)
    assert_raise ArgumentError, fn -> Files.get(ref, files: OtherFiles) end

    renamed = __MODULE__.RenamedFiles

    start_supervised!(%{
      id: renamed,
      start: {Files, :start_link, [[name: renamed, namespace: "stable", store: {:local, dir}]]}
    })

    assert {:ok, "body"} = Files.read(ref, files: renamed)
  end

  test "invalid startup values raise before supervision" do
    opts = [store: :memory, retention_ms: -1]
    assert_raise ArgumentError, ~r/:retention_ms/, fn -> Files.child_spec(opts) end
    assert_raise ArgumentError, ~r/:retention_ms/, fn -> Files.start_link(opts) end

    assert_raise ArgumentError, ~r/:namespace/, fn ->
      Files.child_spec(store: :memory, namespace: nil)
    end
  end

  test "a store open failure returns an error from start_link" do
    assert {:error, {:store, %Slap.SlateDB.Error{kind: :invalid}}} =
             Files.start_link(store: {:url, "bad://bucket"}, name: OtherFiles, namespace: "bad")
  end

  test "using an instance that was not started names the missing instance" do
    assert_raise ArgumentError, ~r/Slap.Files instance .*OtherFiles.* is not started/, fn ->
      Files.get({"doc", "file"}, files: OtherFiles)
    end
  end

  test "invalid instance settings raise before starting workers" do
    objects = Config.get().objects

    assert_raise ArgumentError, ~r/:inline_max_bytes/, fn ->
      Config.new(objects, inline_max_bytes: -1)
    end

    assert_raise ArgumentError, ~r/:sweep_interval_ms/, fn ->
      Config.new(objects, sweep_interval_ms: 0)
    end

    assert_raise ArgumentError, ~r/:inline_max_bytes/, fn ->
      Config.new(objects, inline_max_bytes: 2, inline_limit: 1)
    end
  end

  test "namespaces isolate instances sharing a KV cluster and object store" do
    dir = Path.join(System.tmp_dir!(), "files-shared-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    for {name, namespace, retention, skew} <-
          [{__MODULE__.FilesA, "a", 10, 0}, {__MODULE__.FilesB, "b", 1_000, 500}] do
      start_supervised!(%{
        id: name,
        start:
          {Files, :start_link,
           [
             [
               name: name,
               cluster: Slap.KV.Cluster,
               namespace: namespace,
               store: {:local, dir},
               inline_max_bytes: 1,
               retention_ms: retention,
               max_clock_skew_ms: skew,
               sweep_interval_ms: 3_600_000,
               reconcile_interval_ms: 3_600_000,
               clock: fn -> Agent.get(Slap.Files.Test.FilesCase.Clock, & &1) end
             ]
           ]}
      })
    end

    ref = {"shared", "same"}
    assert {:ok, _} = Files.put(ref, "first A", files: __MODULE__.FilesA)
    assert {:ok, _} = Files.put(ref, "first B", files: __MODULE__.FilesB)
    assert {:ok, "first A"} = Files.read(ref, files: __MODULE__.FilesA)
    assert {:ok, "first B"} = Files.read(ref, files: __MODULE__.FilesB)

    assert {:ok, {_file, old_body}} = Files.stream(ref, files: __MODULE__.FilesB)
    assert {:ok, _} = Files.put(ref, "second B", files: __MODULE__.FilesB)
    assert :ok = Files.delete(ref, files: __MODULE__.FilesA)
    advance(20)
    assert :ok = Sweeper.sweep(__MODULE__.FilesA)
    assert :ok = Sweeper.reconcile(__MODULE__.FilesA)
    assert IO.iodata_to_binary(Enum.to_list(old_body)) == "first B"
    assert {:ok, "second B"} = Files.read(ref, files: __MODULE__.FilesB)
  end

  test "object-store failures return Files availability errors" do
    ref = {"doc", "broken-download"}
    assert {:ok, %FileInfo{storage: :object}} = Files.put(ref, String.duplicate("b", 100))

    broken =
      {:url, "s3://unreachable",
       aws_endpoint: "http://127.0.0.1:1",
       aws_allow_http: true,
       aws_region: "us-east-1",
       aws_access_key_id: "x",
       aws_secret_access_key: "x"}

    bad_name = __MODULE__.BrokenFiles

    start_supervised!(%{
      id: bad_name,
      start:
        {Files, :start_link,
         [
           [
             name: bad_name,
             cluster: Slap.KV.Cluster,
             namespace: "default",
             store: broken,
             sweep_interval_ms: 3_600_000
           ]
         ]}
    })

    assert {:error, read_error} = Files.stream(ref, files: bad_name, timeout: 100)
    assert read_error in [:unavailable, :timeout]
    assert {:error, body_error} = Files.read(ref, files: bad_name, timeout: 100)
    assert body_error in [:unavailable, :timeout]

    assert {:error, write_error} =
             Files.put({"doc", "broken-upload"}, "body",
               storage: :object,
               files: bad_name,
               timeout: 100
             )

    assert write_error in [:unavailable, :timeout]
  end

  describe "inline bodies" do
    test "a small file is stored in its record: put, get, read, list, delete" do
      ref = {"doc", "a"}

      assert {:ok, %FileInfo{storage: :inline, size: 5, version: v1} = file} =
               Files.put(ref, "hello", content_type: "text/plain", metadata: %{"name" => "a.txt"})

      assert file.sha256 == :crypto.hash(:sha256, "hello")
      assert {:ok, %FileInfo{version: ^v1, content_type: "text/plain"}} = Files.get(ref)
      assert {:ok, "hello"} = Files.read(ref)

      assert {:ok, %{files: [%FileInfo{ref: ^ref, version: ^v1, metadata: %{"name" => "a.txt"}}]}} =
               Files.list("doc")

      assert :ok = Files.delete(ref)
      assert {:ok, nil} = Files.get(ref)
      assert {:ok, nil} = Files.read(ref)
      assert objects() == []
    end

    test "a streamed body within inline_max_bytes is stored inline" do
      assert {:ok, %FileInfo{storage: :inline}} = Files.put({"doc", "s"}, ["sm", "all"])
      assert {:ok, "small"} = Files.read({"doc", "s"})
    end

    test "storage: :inline, up to inline_limit" do
      body = String.duplicate("x", 64)
      assert {:ok, %FileInfo{storage: :inline}} = Files.put({"doc", "i"}, body, storage: :inline)

      assert {:error, :too_large_for_inline} =
               Files.put({"doc", "j"}, body <> "x", storage: :inline)

      assert {:error, :too_large_for_inline} =
               Files.put({"doc", "j"}, chunks(body <> "x", 10), storage: :inline)

      assert {:ok, nil} = Files.get({"doc", "j"})
    end
  end

  describe "object bodies" do
    test "a larger file, or storage: :object, is an object" do
      body = :crypto.strong_rand_bytes(100)
      assert {:ok, %FileInfo{storage: :object, size: 100}} = Files.put({"doc", "big"}, body)
      assert {:ok, ^body} = Files.read({"doc", "big"})

      assert {:ok, %FileInfo{storage: :object}} =
               Files.put({"doc", "tiny"}, "t", storage: :object)

      assert {:ok, "t"} = Files.read({"doc", "tiny"})

      advance(1_000)
      sweep()
      assert objects() == Enum.sort([object_key({"doc", "big"}), object_key({"doc", "tiny"})])
      assert Enum.all?(objects(), &String.starts_with?(&1, "objects/ns/ZGVmYXVsdA/"))
      assert intents() == []
    end

    test "a streamed body larger than inline_max_bytes is uploaded as it is read" do
      body = :crypto.strong_rand_bytes(7 * 1024 * 1024)
      assert {:ok, %FileInfo{storage: :object}} = Files.put({"doc", "s"}, chunks(body, 100_000))
      assert {:ok, {%FileInfo{size: size}, stream}} = Files.stream({"doc", "s"})
      assert size == byte_size(body)
      assert :crypto.hash(:sha256, Enum.to_list(stream)) == :crypto.hash(:sha256, body)
    end

    test "a checksum mismatch stores nothing" do
      assert {:error, :checksum_mismatch} =
               Files.put({"doc", "c"}, String.duplicate("x", 100),
                 expected_sha256: :crypto.hash(:sha256, "other")
               )

      assert {:ok, nil} = Files.get({"doc", "c"})
      sweep()
      assert objects() == []
    end
  end

  describe "conditions and retries" do
    test "if_version: :absent creates; a version replaces only that version" do
      ref = {"doc", "v"}
      assert {:ok, %{version: v1}} = Files.put(ref, "one", if_version: :absent)
      assert {:error, {:conflict, ^v1}} = Files.put(ref, "two", if_version: :absent)
      assert {:ok, %{version: v2}} = Files.put(ref, "two", if_version: v1)
      assert {:error, {:conflict, ^v2}} = Files.put(ref, "three", if_version: v1)
      assert {:error, {:conflict, ^v2}} = Files.delete(ref, if_version: v1)
      assert :ok = Files.delete(ref, if_version: v2)
      assert {:error, {:conflict, nil}} = Files.delete(ref, if_version: v2)
    end

    test "a conditional retry conflicts after its write succeeds" do
      body = String.duplicate("y", 100)
      {:ok, first} = Files.put({"doc", "r"}, body, if_version: :absent)

      assert {:error, {:conflict, version}} =
               Files.put({"doc", "r"}, body, if_version: :absent)

      assert version == first.version
      assert {:ok, ^first} = Files.put({"doc", "r"}, body)

      sweep()
      assert objects() == [object_key({"doc", "r"})]
    end

    test "invalid arguments" do
      assert {:error, {:bad_request, :invalid_ref}} = Files.put({"", "a"}, "x")
      assert {:error, {:bad_request, :invalid_partition}} = Files.list(42)
      assert {:error, {:bad_request, :invalid_partition}} = Files.list("")
      assert {:error, {:bad_request, :invalid_body}} = Files.put({"d", "a"}, :body)
      assert {:error, {:bad_request, :invalid_body}} = Files.put({"d", "a"}, ["x", :bad])

      assert {:error, {:bad_request, :invalid_body}} =
               Files.put({"d", "a"}, ["x", :bad], storage: :object)

      assert {:error, {:bad_request, :invalid_metadata}} =
               Files.put({"d", "a"}, "x", metadata: %{a: 1})

      assert {:error, {:bad_request, :invalid_version}} =
               Files.delete({"d", "a"}, if_version: :absent)

      assert {:error, {:bad_request, :invalid_version}} =
               Files.put({"d", "a"}, "x", if_version: nil)

      assert_raise ArgumentError, ~r/:storage/, fn ->
        Files.put({"d", "a"}, "x", storage: :disk)
      end

      assert_raise ArgumentError, ~r/:timeout/, fn ->
        Files.get({"d", "a"}, timeout: -1)
      end

      assert_raise ArgumentError, fn -> Files.put({"d", "a"}, "x", if_vesion: :absent) end

      assert_raise ArgumentError, fn -> Files.delete({"d", "a"}, if_vesion: 1) end

      assert {:ok, nil} = Files.get({"d", "a"})
    end
  end

  describe "replacing and deleting bodies" do
    test "an old body stays readable for retention_ms, then is deleted" do
      ref = {"doc", "rep"}
      {:ok, _} = Files.put(ref, String.duplicate("a", 100))
      old = object_key(ref)
      {:ok, {_file, old_stream}} = Files.stream(ref)

      {:ok, _} = Files.put(ref, String.duplicate("b", 100))
      new = object_key(ref)
      assert new != old

      sweep()
      assert Enum.sort([old, new]) == objects()
      assert IO.iodata_to_binary(Enum.to_list(old_stream)) == String.duplicate("a", 100)

      advance(1_000)
      sweep()
      assert objects() == [new]
      assert {:ok, body} = Files.read(ref)
      assert body == String.duplicate("b", 100)
    end

    test "switching between inline and object bodies" do
      ref = {"doc", "sw"}
      {:ok, %{storage: :object}} = Files.put(ref, String.duplicate("o", 100))
      {:ok, %{storage: :inline}} = Files.put(ref, "inline")
      assert {:ok, "inline"} = Files.read(ref)
      advance(1_000)
      sweep()
      assert objects() == []

      {:ok, %{storage: :object}} = Files.put(ref, String.duplicate("p", 100))
      assert objects() == [object_key(ref)]
    end

    test "a deleted file's object is deleted after retention_ms" do
      ref = {"doc", "del"}
      {:ok, _} = Files.put(ref, String.duplicate("d", 100))
      key = object_key(ref)
      :ok = Files.delete(ref)
      assert {:ok, nil} = Files.get(ref)

      sweep()
      assert objects() == [key]
      advance(1_000)
      sweep()
      assert objects() == []
      assert intents() == []
    end
  end
end
