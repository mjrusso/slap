defmodule Slap.SlateDB.ObjectStoreTest do
  use ExUnit.Case, async: true

  alias Slap.SlateDB.ObjectStore

  defp exercise(store) do
    key = "objects/#{System.unique_integer([:positive])}"

    assert ObjectStore.get(store, key) == {:ok, nil}
    assert {:ok, v1} = ObjectStore.put(store, key, "one", mode: :create)
    assert {:ok, {"one", ^v1}} = ObjectStore.get(store, key)
    assert ObjectStore.put(store, key, "again", mode: :create) == {:error, :conflict}

    assert {:ok, v2} = ObjectStore.put(store, key, "two", mode: {:update, v1})
    assert v2 != v1
    assert ObjectStore.put(store, key, "three", mode: {:update, v1}) == {:error, :conflict}
    assert {:ok, {"two", ^v2}} = ObjectStore.get(store, key)

    assert {:ok, _} = ObjectStore.put(store, key, "four")
    assert {:ok, keys} = ObjectStore.list(store, "objects/")
    assert key in keys

    assert :ok = ObjectStore.delete(store, key)
    assert :ok = ObjectStore.delete(store, key)
    assert ObjectStore.get(store, key) == {:ok, nil}
    # An update of an object that is gone.
    assert ObjectStore.put(store, key, "five", mode: {:update, v2}) == {:error, :conflict}
  end

  # Uploads and downloads: a small body (one PUT), an empty one, a large one
  # (a multipart upload, in chunks that do not line up with its parts) from
  # a stream that can only be read once, and one whose stream raises.
  defp exercise_streams(store) do
    key = "bodies/#{System.unique_integer([:positive])}"
    assert ObjectStore.download(store, key) == {:ok, nil}

    assert {:ok, _} = ObjectStore.upload(store, key, ["sm", "all"])
    assert {:ok, {chunks, 5, _version}} = ObjectStore.download(store, key)
    assert Enum.join(chunks) == "small"

    assert {:ok, _} = ObjectStore.upload(store, key <> "-empty", [])
    assert {:ok, {chunks, 0, _version}} = ObjectStore.download(store, key <> "-empty")
    assert Enum.join(chunks) == ""

    big = :crypto.strong_rand_bytes(12 * 1024 * 1024 + 7)
    assert {:ok, version} = ObjectStore.upload(store, key, once(chunks(big, 700_001)))
    assert {:ok, {chunks, size, ^version}} = ObjectStore.download(store, key)
    assert size == byte_size(big)
    assert sha(Enum.to_list(chunks)) == sha(big)

    # The stream raises after 6 MiB, once the multipart upload has started:
    # it is aborted, and the object is as it was.
    failing =
      Stream.concat(chunks(:binary.copy("x", 6 * 1024 * 1024), 1024 * 1024), [:boom])
      |> Stream.map(fn
        :boom -> raise "the body failed"
        chunk -> chunk
      end)

    assert_raise RuntimeError, "the body failed", fn ->
      ObjectStore.upload(store, key, failing)
    end

    assert {:ok, {_chunks, ^size, ^version}} = ObjectStore.download(store, key)
    assert {:ok, keys} = ObjectStore.list(store, "bodies/")
    assert Enum.sort(keys) == Enum.sort([key, key <> "-empty"])
  end

  defp chunks(binary, size) do
    for i <- 0..div(byte_size(binary) - 1, size),
        do: binary_part(binary, i * size, min(size, byte_size(binary) - i * size))
  end

  # A stream that raises if it is enumerated twice, like a request body.
  defp once(list) do
    started = :counters.new(1, [])

    Stream.resource(
      fn ->
        :counters.add(started, 1, 1)
        if :counters.get(started, 1) > 1, do: raise("read twice")
        list
      end,
      fn
        [] -> {:halt, []}
        [chunk | rest] -> {[chunk], rest}
      end,
      fn _ -> :ok end
    )
  end

  defp sha(iodata), do: :crypto.hash(:sha256, iodata)

  test "in memory" do
    {:ok, store} = ObjectStore.open("root", store: :memory)
    exercise(store)
    exercise_streams(store)
  end

  @tag :tmp_dir
  test "on the local file system: create-if-absent, but no updates", %{tmp_dir: dir} do
    {:ok, store} = ObjectStore.open("root", store: {:local, dir})
    assert {:ok, v1} = ObjectStore.put(store, "a", "one", mode: :create)
    assert ObjectStore.put(store, "a", "again", mode: :create) == {:error, :conflict}
    assert ObjectStore.put(store, "a", "two", mode: {:update, v1}) == {:error, :unsupported}
    assert {:ok, ["a"]} = ObjectStore.list(store, "")
    assert File.exists?(Path.join([dir, "root", "a"]))
    exercise_streams(store)
  end

  @tag :s3
  test "on S3" do
    store =
      {:url, "s3://#{System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")}/objects",
       [
         aws_endpoint: System.fetch_env!("SLAP_TEST_S3_ENDPOINT"),
         aws_allow_http: "true",
         aws_region: System.get_env("SLAP_TEST_S3_REGION", "us-east-1"),
         aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
         aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
       ]}

    {:ok, store} = ObjectStore.open("root-#{System.unique_integer([:positive])}", store: store)
    exercise(store)
    exercise_streams(store)
  end

  test "invalid modes" do
    {:ok, store} = ObjectStore.open("", store: :memory)
    assert_raise ArgumentError, fn -> ObjectStore.put(store, "k", "v", mode: :bogus) end
  end
end
