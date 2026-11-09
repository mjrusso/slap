defmodule Slap.Yjs.Reconstruction do
  @moduledoc false

  alias Slap.Cluster.Inspection.Read
  alias Slap.Yjs.{Frame, Updates}

  @defaults %{input_bytes: 8 * 1024 * 1024, entries: 50_000, output_bytes: 1024 * 1024}
  @maximums %{input_bytes: 32 * 1024 * 1024, entries: 200_000, output_bytes: 4 * 1024 * 1024}
  @types [:text, :array, :map, :xml_fragment]

  def limits, do: %{defaults: @defaults, maximums: @maximums}

  def run(%{snapshot: snapshot, updates: entries, roots: roots} = input, deadline)
      when (is_binary(snapshot) or is_nil(snapshot)) and is_list(entries) and is_list(roots) do
    with {:ok, limits} <- requested_limits(Map.get(input, :limits, %{})),
         :ok <- check_roots(roots),
         :ok <- check_input(snapshot, entries, limits),
         :ok <- Read.check_deadline(deadline) do
      reconstruct(snapshot, entries, roots, limits, deadline)
    end
  end

  def run(_input, _deadline), do: {:error, :invalid_reconstruction_input}

  defp requested_limits(requested) when is_map(requested) do
    limits = Map.merge(@defaults, requested)

    if Enum.all?(limits, fn {key, value} ->
         is_integer(value) and value > 0 and value <= Map.get(@maximums, key, 0)
       end),
       do: {:ok, limits},
       else: {:error, :invalid_reconstruction_limits}
  end

  defp requested_limits(_requested), do: {:error, :invalid_reconstruction_limits}

  defp check_roots(roots) do
    if Enum.all?(roots, &valid_root?/1) and
         length(Enum.uniq_by(roots, &elem(&1, 0))) == length(roots),
       do: :ok,
       else: {:error, :invalid_root_interpretations}
  end

  defp valid_root?({name, type}), do: is_binary(name) and String.valid?(name) and type in @types
  defp valid_root?(_root), do: false

  defp check_input(snapshot, entries, limits) do
    if Enum.all?(entries, &is_binary/1) do
      # Snapshots published by SnapshotLog are one message. A concatenated legacy
      # snapshot can have more headers; collection enforces its full wire size.
      snapshot_bytes = if is_nil(snapshot), do: 0, else: byte_size(snapshot) + 4
      bytes = snapshot_bytes + Enum.reduce(entries, 0, &(byte_size(&1) + 4 + &2))

      cond do
        bytes > limits.input_bytes ->
          {:error, {:too_large, bytes, limits.input_bytes}}

        length(entries) > limits.entries ->
          {:error, {:too_large, length(entries), limits.entries}}

        true ->
          :ok
      end
    else
      {:error, :invalid_reconstruction_input}
    end
  end

  # Parsed one frame at a time, as they are applied: an entry can hold many
  # empty frames, and each costs a list cell if parsed ahead.
  defp updates(entries) do
    Stream.flat_map(entries, &Stream.unfold(&1, fn rest -> next_frame(rest) end))
  end

  defp next_frame(nil), do: nil

  defp next_frame(binary) do
    case Frame.next(binary) do
      {:ok, update, rest} -> {update, rest}
      :done -> nil
      {:error, reason} -> {{:error, {:invalid_frames, reason}}, nil}
    end
  end

  defp reconstruct(snapshot, entries, roots, limits, deadline) do
    doc = Yex.Doc.new()
    updates = Stream.concat(List.wrap(snapshot), updates(entries))

    with :ok <- Updates.apply(doc, updates, :inspection, deadline),
         :ok <- Read.check_deadline(deadline),
         {:ok, pending} <- Yex.Doc.get_pending_update(doc),
         {:ok, deletes} <- Yex.Doc.get_pending_ds(doc),
         {:ok, content} <- content(doc, roots, deadline),
         :ok <- Read.check_deadline(deadline) do
      json = JSON.encode!(content)

      cond do
        byte_size(json) > limits.output_bytes ->
          {:error, {:truncated, byte_size(json), limits.output_bytes}}

        Read.expired?(deadline) ->
          {:error, :deadline}

        true ->
          {:ok,
           %{
             content: {:json, json},
             pending: %{updates: byte_size(pending || <<>>), deletes: byte_size(deletes || <<>>)}
           }}
      end
    end
  end

  defp content(doc, roots, deadline) do
    available = Map.new(Yex.Doc.root_refs(doc))
    requested = Map.new(roots)

    available
    |> Map.keys()
    |> Enum.concat(Map.keys(requested))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce_while({:ok, [], %{doc: doc, update: nil, copies: %{}}}, fn name,
                                                                              {:ok, acc, probes} ->
      case Read.check_deadline(deadline) do
        :ok ->
          {root, probes} = root(probes, name, available, requested)
          {:cont, {:ok, [root | acc], probes}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, roots, _probes} -> {:ok, %{roots: Enum.reverse(roots)}}
      error -> error
    end
  end

  defp root(probes, name, available, requested) do
    case {Map.fetch(available, name), Map.fetch(requested, name)} do
      {:error, _} ->
        {%{name: name, status: :absent}, probes}

      {{:ok, type}, :error} ->
        {holds, probes} = holds(probes, name)
        {uninterpreted(probes.doc, name, type, holds), probes}

      {{:ok, _type}, {:ok, type}} ->
        interpret(probes, name, type)
    end
  end

  defp uninterpreted(doc, name, _type, holds) when holds in [:map, :text],
    do: %{name: name, interpretation: holds, value: value(doc, name, holds), inferred: true}

  defp uninterpreted(_doc, name, type, nil), do: %{name: name, type: type, status: :uninterpreted}

  defp uninterpreted(_doc, name, type, holds),
    do: %{name: name, type: type, status: :uninterpreted, holds: holds}

  # An empty result can mean either empty content or an incompatible root type.
  defp interpret(probes, name, type) do
    case value(probes.doc, name, type) do
      empty when empty in ["", [], %{}] ->
        case holds(probes, name) do
          {nil, probes} ->
            {%{name: name, interpretation: type, value: empty}, probes}

          {:sequence, probes} when type in [:text, :xml_fragment] ->
            {%{name: name, interpretation: type, status: :ambiguous, holds: :sequence}, probes}

          {holds, probes} ->
            {%{name: name, interpretation: type, status: :wrong_type, holds: holds}, probes}
        end

      value ->
        {%{name: name, interpretation: type, value: value}, probes}
    end
  end

  # Replayed roots do not retain their declared types. Map entries and text
  # identify those types; arrays, XML and embed-only text can all produce a
  # nonempty array with an empty text result.
  defp holds(probes, name) do
    Enum.reduce_while([:map, :text, :sequence], {nil, probes}, fn holds, {nil, probes} ->
      {copy, probes} = probe(probes, holds)
      if size(copy, name, holds) > 0, do: {:halt, {holds, probes}}, else: {:cont, {nil, probes}}
    end)
  end

  # Reading a root fixes its type in that document. Probe each type in a
  # separate copy of the state.
  defp probe(%{update: nil} = probes, holds),
    do: probe(%{probes | update: Yex.encode_state_as_update!(probes.doc)}, holds)

  defp probe(probes, holds) do
    case probes.copies do
      %{^holds => copy} ->
        {copy, probes}

      copies ->
        copy = Yex.Doc.new()
        :ok = Yex.apply_update(copy, probes.update)
        {copy, %{probes | copies: Map.put(copies, holds, copy)}}
    end
  end

  defp size(copy, name, :map), do: copy |> Yex.Doc.get_map(name) |> Yex.Map.size()
  # Use the rendered string's size; Yex.Text.length/1 also counts array items.
  defp size(copy, name, :text),
    do: copy |> Yex.Doc.get_text(name) |> Yex.Text.to_string() |> byte_size()

  defp size(copy, name, :sequence), do: copy |> Yex.Doc.get_array(name) |> Yex.Array.length()

  defp value(doc, name, :text), do: doc |> Yex.Doc.get_text(name) |> Yex.Text.to_string()

  defp value(doc, name, :array),
    do: doc |> Yex.Doc.get_array(name) |> Yex.Array.to_json() |> plain()

  defp value(doc, name, :map), do: doc |> Yex.Doc.get_map(name) |> Yex.Map.to_json() |> plain()

  defp value(doc, name, :xml_fragment),
    do: doc |> Yex.Doc.get_xml_fragment(name) |> Yex.XmlFragment.to_string()

  defp plain(%Yex.Buffer{data: bytes}), do: %{"$bytes" => Base.encode16(bytes, case: :lower)}
  defp plain(value) when is_list(value), do: Enum.map(value, &plain/1)

  defp plain(value) when is_map(value),
    do: Map.new(value, fn {key, value} -> {escape_key(key), plain(value)} end)

  defp plain(:undefined), do: nil
  defp plain(value), do: value

  defp escape_key("$" <> _rest = key), do: "$" <> key
  defp escape_key(key), do: key
end
