defmodule Slap.Yjs.IntrospectTest do
  # Reconstruction shares the node-wide inspection slot.
  use ExUnit.Case, async: false
  alias Slap.Yjs.{Frame, Introspect, Reconstruction}

  test "reports structure without reconstructing content" do
    assert {:ok, %{frame_count: 2, frame_sizes: [2, 3], bytes: 7}} =
             Introspect.decode(Frame.frames([<<0, 255>>, "abc"]))

    assert {:error, :truncated} = Introspect.decode(<<4, 1>>)
    assert {:error, :invalid} = Introspect.decode(:binary.copy(<<255>>, 10))
  end

  test "freshly replayed roots use explicit interpretations and keep buffer types" do
    doc = Yex.Doc.new()
    map = Yex.Doc.get_map(doc, "map")
    :ok = Yex.Map.set(map, "text", "hello")
    :ok = Yex.Map.set(map, "bytes", Yex.Buffer.from(<<255, 0>>))
    :ok = Yex.Map.set(map, "empty_buffer", Yex.Buffer.from(<<>>))
    :ok = Yex.Map.set(map, "empty_array", [])
    :ok = Yex.Map.set(map, "$bytes", "user value")
    :ok = Yex.Map.set(map, "$$x", "escaped")
    :ok = Yex.Map.set(map, "nested", [%{"$x" => %{"$bytes" => "nested user value"}}])
    :ok = Yex.Text.insert(Yex.Doc.get_text(doc, "$root"), 0, "root name")
    :ok = Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "hello\e[31m")
    :ok = Yex.Array.push(Yex.Doc.get_array(doc, "array"), "item")

    :ok =
      Yex.XmlFragment.insert(
        Yex.Doc.get_xml_fragment(doc, "xml"),
        0,
        Yex.XmlTextPrelim.from("xml")
      )

    update = Yex.encode_state_as_update!(doc)
    input = %{snapshot: nil, updates: [Frame.frame(update)], roots: []}
    assert {:ok, %{content: {:json, json}}} = reconstruct(input)
    assert Enum.all?(JSON.decode!(json)["roots"], &(&1["type"] == "untyped"))

    roots = [
      {"map", :map},
      {"text", :text},
      {"array", :array},
      {"xml", :xml_fragment},
      {"absent", :text},
      {"$root", :text}
    ]

    assert {:ok, %{content: {:json, json}, pending: %{updates: 0, deletes: 0}}} =
             reconstruct(%{input | roots: roots})

    values = Map.new(JSON.decode!(json)["roots"], &{&1["name"], &1})

    assert values["map"]["value"] == %{
             "text" => "hello",
             "bytes" => %{"$bytes" => "ff00"},
             "empty_buffer" => %{"$bytes" => ""},
             "empty_array" => [],
             "$$bytes" => "user value",
             "$$$x" => "escaped",
             "nested" => [%{"$$x" => %{"$$bytes" => "nested user value"}}]
           }

    assert values["$root"]["value"] == "root name"
    assert values["text"]["value"] == "hello\e[31m"
    assert values["array"]["value"] == ["item"]
    assert values["xml"]["value"] == "xml"
    assert values["absent"] == %{"name" => "absent", "status" => "absent"}
  end

  test "reconstruction applies each frame as it is parsed and stops at the first bad one" do
    # A million empty frames, then a truncated one: the first empty frame is
    # not a valid update, so nothing after it is parsed.
    entry = :binary.copy(<<0>>, 1_000_000) <> <<5>>

    assert {:error, {:encoding_exception, _}} =
             reconstruct(%{snapshot: nil, updates: [entry], roots: []})
  end

  test "limits reject input before decoding and output after conversion" do
    input = %{snapshot: nil, updates: [<<255, 255>>], roots: []}
    assert {:error, {:too_large, 6, 1}} = reconstruct(Map.put(input, :limits, %{input_bytes: 1}))

    assert {:error, {:too_large, 2, 1}} =
             reconstruct(%{input | updates: [<<0>>, <<0>>]} |> Map.put(:limits, %{entries: 1}))

    assert {:error, :invalid_reconstruction_limits} =
             reconstruct(Map.put(input, :limits, %{input_bytes: 33 * 1024 * 1024}))

    assert {:error, {:invalid_frames, :truncated}} = reconstruct(input)

    assert {:error, {:truncated, _, 1}} =
             reconstruct(%{input | updates: []} |> Map.put(:limits, %{output_bytes: 1}))

    assert {:error, :deadline} =
             Reconstruction.run(
               %{input | updates: []},
               System.monotonic_time(:millisecond) - 1
             )
  end

  test "input limits include each entry header and the snapshot header" do
    doc = Yex.Doc.new()
    :ok = Yex.Text.insert(Yex.Doc.get_text(doc, "body"), 0, "stored")
    snapshot = Yex.encode_state_as_update!(doc)
    entry = Frame.frame(snapshot)
    input = %{snapshot: snapshot, updates: [entry], roots: []}
    wire_bytes = byte_size(snapshot) + byte_size(entry) + 8

    assert {:ok, _} = reconstruct(Map.put(input, :limits, %{input_bytes: wire_bytes}))

    assert {:error, {:too_large, ^wire_bytes, limit}} =
             reconstruct(Map.put(input, :limits, %{input_bytes: wire_bytes - 1}))

    assert limit == wire_bytes - 1
  end

  test "a replayed map root can be projected as text without a type error" do
    doc = Yex.Doc.new()
    :ok = Yex.Map.set(Yex.Doc.get_map(doc, "map"), "key", "value")
    :ok = Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "hello")

    input = %{
      snapshot: Yex.encode_state_as_update!(doc),
      updates: [],
      roots: [{"map", :text}, {"text", :text}]
    }

    assert {:ok, %{content: {:json, json}}} = reconstruct(input)

    assert JSON.decode!(json)["roots"] == [
             %{"name" => "map", "interpretation" => "text", "value" => ""},
             %{"name" => "text", "interpretation" => "text", "value" => "hello"}
           ]

    assert {:ok, %{content: {:json, json}}} = reconstruct(%{input | roots: [{"map", :map}]})

    assert Enum.find(JSON.decode!(json)["roots"], &(&1["name"] == "map"))["value"] == %{
             "key" => "value"
           }
  end

  test "reconstruction reports updates waiting for missing predecessors" do
    doc = Yex.Doc.new()
    root = Yex.Doc.get_text(doc, "text")
    :ok = Yex.Text.insert(root, 0, "x")
    {:ok, clock} = Yex.encode_state_vector(doc)
    :ok = Yex.Text.insert(root, 1, "p")
    update = Yex.encode_state_as_update!(doc, clock)
    input = %{snapshot: nil, updates: [Frame.frame(update)], roots: [{"text", :text}]}
    assert {:ok, %{pending: %{updates: pending}}} = reconstruct(input)
    assert pending > 0
  end

  defp reconstruct(input), do: Introspect.reconstruct(1, 5000, input)
end
