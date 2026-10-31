defmodule Slap.Files.IntrospectTest do
  use ExUnit.Case, async: true
  alias Slap.Files.{Codec, Config, Introspect}
  alias Slap.KV.Keys

  test "recognizes namespace families and preserves intent state" do
    config = %Config{objects: nil, namespace: <<0, 255>>}

    for family <- [:record, :intent, :object] do
      assert {:ok, %{namespace: <<0, 255>>, family: ^family, suffix: "p:7"}} =
               Config.parse_partition(Config.partition(config, family, "p:7"))
    end

    partition = Config.partition(config, :intent, "7")
    intent = %{ref: {"p", "id"}, keys: ["object"], due_ms: 123, taken: :writer}

    assert {:ok, %{namespace: <<0, 255>>, family: :intent, value: %{taken: :writer}}} =
             Introspect.decode(
               Keys.encode(partition, "id"),
               Codec.encode(:intent, intent)
             )

    assert {:error, _} = Config.parse_partition("files-ns:!:record:p")
    assert {:error, _} = Introspect.decode(Keys.encode("files-ns:ZA:record:p", "id"), "bad")
  end

  test "inspects encoded file records without exposing inline bodies" do
    key = Keys.encode("files-ns:ZA:record:p", "id")

    for {body, storage, summary} <- [
          {{:inline, "body"}, :inline, [:inline, %{bytes: 4}]},
          {{:object, "object-key"}, :object, [:object, "object-key"]}
        ] do
      record = %{body: body, size: 4, sha256: "hash"}
      encoded = Codec.encode(:file, record)
      assert Codec.decode(:file, encoded) == record

      assert {:ok, %{value: %{body: ^summary, storage: ^storage, size: 4}}} =
               Introspect.decode(key, encoded)
    end
  end

  test "rejects unsupported versions and malformed or unbounded values" do
    key = Keys.encode("files-ns:ZA:record:p", "id")
    record = %{body: {:inline, "body"}, size: 4, sha256: "hash"}

    for tag <- [:file, :intent] do
      assert {:error, {:unsupported_version, 2}} =
               Introspect.decode(key, :erlang.term_to_binary({tag, 2, record}))
    end

    assert {:error, :malformed_files_record} =
             Introspect.decode(key, :erlang.term_to_binary({:other, 1, record}))

    assert {:error, :malformed_files_record} =
             Introspect.decode(key, Codec.encode(:intent, record))

    assert {:error, :malformed_files_record} =
             Introspect.decode(key, Codec.encode(:file, %{}))

    assert {:error, :malformed_files_record} =
             Introspect.decode(key, :erlang.term_to_binary(record))

    large = %{record | body: {:inline, String.duplicate("x", 512 * 1024)}}

    assert {:error, :oversized_value} = Introspect.decode(key, Codec.encode(:file, large))

    assert {:error, :compressed_etf} =
             Introspect.decode(key, :erlang.term_to_binary({:file, 1, large}, [:compressed]))
  end
end
