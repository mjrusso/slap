defmodule Slap.Files.ModelTest do
  # Random command sequences against Slap.Files and a model (a map of files):
  # puts of inline and object bodies (by size and by :storage), with and
  # without conditions and as retries of the current content, deletes,
  # gets and reads, sweeps, reconciliations, and the clock moving past
  # retention and upload deadlines and the max_clock_skew_ms the sweeper
  # waits after them. Every result must match. After each sequence every
  # file is deleted and the clock moved past every deadline: a sweep must
  # then leave no object, no intent and no registration.
  #
  # Versions are chosen by Slap.KV, so the model learns each from the write
  # that returned it. SLAP_FILES_PROP_RUNS (default 50) sets the number of
  # sequences; `mix test --seed N` replays a run.
  use Slap.Files.Test.FilesCase, async: false
  use ExUnitProperties

  alias Slap.Files.File, as: FileInfo
  alias Slap.Files.Sweeper

  @moduletag :capture_log
  @moduletag files: [max_clock_skew_ms: 500]
  @moduletag timeout: :infinity

  @commands 30
  @ids ~w(a b c)

  property "Files behaves like the model" do
    runs = String.to_integer(System.get_env("SLAP_FILES_PROP_RUNS", "50"))
    seeds = list_of(integer(0..0xFFFF_FFFF), min_length: 1, max_length: @commands)

    check all(seeds <- seeds, max_runs: runs, initial_size: @commands) do
      partition = "run#{System.unique_integer([:positive])}"

      model =
        seeds
        |> Enum.with_index(1)
        |> Enum.reduce(%{}, fn {seed, i}, model ->
          :rand.seed(:exsss, {seed, 0, 17})
          command = command(model)
          actual = execute(command, partition)
          {expected, model} = expect(model, command, actual)

          assert actual == expected, """
          command #{i}: #{inspect(command)}
          expected: #{inspect(expected)}
          got:      #{inspect(actual)}
          """

          model
        end)

      for id <- Map.keys(model), do: :ok = Files.delete({partition, id})
      advance(60_000)
      sweep()
      assert objects() == []
      assert intents() == []
      assert registrations() == []
    end
  end

  property "two namespaces on one cluster keep independent files", %{files_dir: dir} do
    other = __MODULE__.OtherFiles

    start_supervised!(%{
      id: other,
      start:
        {Files, :start_link,
         [
           [
             name: other,
             cluster: Slap.KV.Cluster,
             namespace: "model-other",
             store: {:local, dir},
             inline_max_bytes: 1,
             retention_ms: 1_000,
             sweep_interval_ms: 3_600_000,
             reconcile_interval_ms: 3_600_000,
             max_clock_skew_ms: 500,
             clock: fn -> Agent.get(Slap.Files.Test.FilesCase.Clock, & &1) end
           ]
         ]}
    })

    seeds = list_of(integer(0..0xFFFF_FFFF), min_length: 1, max_length: 20)

    check all(seeds <- seeds, max_runs: 10) do
      ref = {"namespaces-#{System.unique_integer([:positive])}", "same"}

      model =
        Enum.reduce(seeds, %{Slap.Files => nil, other => nil}, fn seed, model ->
          :rand.seed(:exsss, {seed, 1, 23})
          target = Enum.random([Slap.Files, other])

          updated =
            if rem(seed, 3) == 0 do
              assert :ok = Files.delete(ref, files: target)
              Map.put(model, target, nil)
            else
              body = "body-#{seed}"
              assert {:ok, _} = Files.put(ref, body, files: target, storage: :object)
              Map.put(model, target, body)
            end

          for name <- [Slap.Files, other] do
            assert Files.read(ref, files: name) == {:ok, updated[name]}
          end

          updated
        end)

      for name <- Map.keys(model), do: assert(:ok = Files.delete(ref, files: name))
      advance(60_000)
      assert :ok = Sweeper.sweep()
      assert :ok = Sweeper.sweep(other)
      assert :ok = Sweeper.reconcile()
      assert :ok = Sweeper.reconcile(other)
      assert objects() == []
    end
  end

  defp command(model) do
    id = Enum.random(@ids)

    case :rand.uniform(100) do
      n when n <= 38 ->
        {:put, id, body(model, id), Enum.random([:auto, :auto, :object, :inline]),
         condition(model, id)}

      n when n <= 53 ->
        {:delete, id, with(:absent <- condition(model, id), do: nil)}

      n when n <= 56 ->
        invalid_command(id)

      n when n <= 88 ->
        Enum.random([{:get, id}, {:get, id}, {:read, id}, {:read, id}, {:list}])

      n when n <= 94 ->
        {:advance, Enum.random([100, 1_000, 20_000])}

      n when n <= 97 ->
        {:sweep}

      _ ->
        {:reconcile}
    end
  end

  defp invalid_command(id) do
    Enum.random([
      {:bad_option, Enum.random([:put, :delete]), id},
      {:nil_version, id},
      {:invalid_body, id}
    ])
  end

  # Mostly new bodies, of sizes around inline_max_bytes (16) and past
  # inline_limit (64); sometimes the file's current one, as a retry.
  defp body(model, id) do
    case {model[id], :rand.uniform(4)} do
      {%{body: body}, 1} -> body
      _ -> :crypto.strong_rand_bytes(Enum.random([0, 5, 16, 17, 40, 70]))
    end
  end

  defp condition(model, id) do
    case {model[id], :rand.uniform(4)} do
      {_, 1} -> :absent
      {%{version: v}, 2} -> v
      {%{version: v}, 3} -> v + 1
      _ -> nil
    end
  end

  defp execute({:put, id, body, storage, condition}, partition) do
    opts = [storage: storage] ++ if(condition, do: [if_version: condition], else: [])

    case Files.put({partition, id}, body, opts) do
      {:ok, %FileInfo{} = file} -> {:ok, {file.version, file.storage, file.size}}
      other -> other
    end
  end

  defp execute({:delete, id, condition}, partition),
    do: Files.delete({partition, id}, if(condition, do: [if_version: condition], else: []))

  defp execute({:bad_option, :put, id}, partition) do
    assert_raise ArgumentError, fn -> Files.put({partition, id}, "bad", if_vesion: :absent) end
    :bad_option
  end

  defp execute({:bad_option, :delete, id}, partition) do
    assert_raise ArgumentError, fn -> Files.delete({partition, id}, if_vesion: 0) end
    :bad_option
  end

  defp execute({:nil_version, id}, partition),
    do: Files.put({partition, id}, "body", if_version: nil)

  defp execute({:invalid_body, id}, partition),
    do: Files.put({partition, id}, ["valid", :invalid])

  defp execute({:get, id}, partition) do
    case Files.get({partition, id}) do
      {:ok, nil} -> {:ok, nil}
      {:ok, file} -> {:ok, {file.version, file.size}}
    end
  end

  defp execute({:list}, partition) do
    case Files.list(partition) do
      {:ok, %{files: files}} ->
        {:ok, Map.new(files, fn file -> {elem(file.ref, 1), {file.version, file.size}} end)}

      error ->
        error
    end
  end

  defp execute({:read, id}, partition), do: Files.read({partition, id})

  defp execute({:advance, ms}, _partition), do: advance(ms)
  defp execute({:sweep}, _partition), do: sweep()
  defp execute({:reconcile}, _partition), do: reconcile()

  # The expected result, given the actual one where the model cannot know
  # it (a new version), and the model after the command.
  defp expect(model, {:bad_option, _kind, _id}, _actual),
    do: {:bad_option, model}

  defp expect(model, {:nil_version, _id}, _actual),
    do: {{:error, {:bad_request, :invalid_version}}, model}

  defp expect(model, {:invalid_body, _id}, _actual),
    do: {{:error, {:bad_request, :invalid_body}}, model}

  defp expect(model, {:put, id, body, storage, condition}, actual) do
    current = model[id]

    cond do
      storage == :inline and byte_size(body) > 64 ->
        {{:error, :too_large_for_inline}, model}

      not holds?(current, condition) ->
        {{:error, {:conflict, current && current.version}}, model}

      current != nil and current.body == body and
          current.storage == where(storage, byte_size(body)) ->
        {{:ok, {current.version, current.storage, byte_size(body)}}, model}

      true ->
        written(model, id, body, where(storage, byte_size(body)), actual)
    end
  end

  defp expect(model, {:delete, id, condition}, _actual) do
    current = model[id]

    cond do
      current == nil and condition == nil -> {:ok, model}
      holds?(current, condition) -> {:ok, Map.delete(model, id)}
      true -> {{:error, {:conflict, current && current.version}}, model}
    end
  end

  defp expect(model, {:get, id}, _actual) do
    case model[id] do
      nil -> {{:ok, nil}, model}
      file -> {{:ok, {file.version, byte_size(file.body)}}, model}
    end
  end

  defp expect(model, {:list}, _actual) do
    rows = Map.new(model, fn {id, file} -> {id, {file.version, byte_size(file.body)}} end)
    {{:ok, rows}, model}
  end

  defp expect(model, {:read, id}, _actual), do: {{:ok, model[id] && model[id].body}, model}
  defp expect(model, {:advance, _}, _actual), do: {:ok, model}
  defp expect(model, {:sweep}, _actual), do: {:ok, model}
  defp expect(model, {:reconcile}, _actual), do: {:ok, model}

  defp written(model, id, body, stored, {:ok, {version, stored, _}} = actual) do
    if model[id] == nil or version > model[id].version,
      do: {actual, Map.put(model, id, %{body: body, version: version, storage: stored})},
      else: {{:ok, {:a_new_version, stored, byte_size(body)}}, model}
  end

  defp written(model, _id, body, stored, _actual),
    do: {{:ok, {:a_new_version, stored, byte_size(body)}}, model}

  defp holds?(_current, nil), do: true
  defp holds?(nil, :absent), do: true
  defp holds?(%{version: v}, v), do: true
  defp holds?(_current, _condition), do: false

  defp where(:auto, size) when size <= 16, do: :inline
  defp where(:auto, _size), do: :object
  defp where(storage, _size), do: storage
end
