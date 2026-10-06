defmodule Slap.KV.ModelTest do
  # Random command sequences against Slap.KV and a model (a map of rows):
  # every result must match. Includes conditional writes with current,
  # stale and unknown versions, writes with deadlines (a passed deadline is
  # never applied), linearizable and durable reads, scans with prefixes and pages, replaced
  # partition writers, and restarting the cluster, so state must reload from
  # storage. (Commands run one at a time; test/durability_test.exs covers
  # writes in flight.)
  #
  # Versions are chosen by SlateDB, so the model learns each one from the
  # write that returned it, and checks that it is higher than any version
  # returned before in its partition (versions increase within a shard).
  #
  # Each command is chosen from the model's state and a seed; StreamData
  # generates the seeds, so a failure shrinks to the shortest sequence of
  # commands that fails. SLAP_KV_PROP_RUNS (default 100) sets the number of
  # sequences; `mix test --seed N` replays a run.
  use Slap.KV.Test.ClusterCase, async: false
  use ExUnitProperties

  @moduletag :capture_log
  @moduletag timeout: :infinity

  @commands 40
  @partitions ~w(p1 p2 p3)
  @keys ~w(a ab b ba c)
  @prefixes ["", "a", "b", "z"]

  property "KV behaves like the model", context do
    runs = String.to_integer(System.get_env("SLAP_KV_PROP_RUNS", "100"))
    seeds = list_of(integer(0..0xFFFF_FFFF), min_length: 1, max_length: @commands)

    check all(seeds <- seeds, max_runs: runs, initial_size: @commands) do
      run = "r#{System.unique_integer([:positive])}/"
      start = %{rows: %{}, max_version: %{}, seen: []}

      seeds
      |> Enum.with_index(1)
      |> Enum.reduce({start, []}, fn {seed, i}, {model, history} ->
        :rand.seed(:exsss, {seed, 0, 17})
        command = command(model)
        history = [command | history]
        actual = execute(command, run, context)
        {expected, model} = expect(model, command, actual)

        assert actual == expected, """
        command #{i}: #{inspect(command)}
        expected: #{inspect(expected)}
        got:      #{inspect(actual)}
        history (latest first): #{inspect(Enum.take(history, 10))}
        """

        {model, history}
      end)
    end
  end

  # -- Commands ---------------------------------------------------------------

  defp command(model) do
    p = Enum.random(@partitions)
    k = Enum.random(@keys)

    case :rand.uniform(100) do
      n when n <= 33 ->
        {:put, p, k, value(), condition(model, p, k, true), deadline()}

      n when n <= 48 ->
        {:delete, p, k, condition(model, p, k, false), deadline()}

      n when n <= 51 ->
        invalid_command(p, k)

      n when n <= 75 ->
        {:get, p, k, Enum.random([:durable, :linearizable])}

      n when n <= 93 ->
        {:scan, p, Enum.random(@prefixes), Enum.random(1..4), Enum.random([false, true])}

      n when n <= 99 ->
        {:restart_partition_writer, p}

      _ ->
        {:restart}
    end
  end

  defp invalid_command(p, k) do
    Enum.random([
      {:bad_option, Enum.random([:put, :delete]), p, k},
      {:put_nil_version, p, k},
      {:invalid_control, p, k}
    ])
  end

  defp deadline, do: Enum.random([nil, nil, :later, :passed])

  defp value, do: :crypto.strong_rand_bytes(:rand.uniform(4))

  # :any, :absent (for a put), the row's version, or any version seen so
  # far (usually another row's, or one this row no longer has).
  defp condition(model, p, k, put?) do
    current = model.rows[{p, k}]

    choices =
      [:any, :seen] ++
        if(put?, do: [:absent], else: []) ++ if(current, do: [:current, :current], else: [])

    case Enum.random(choices) do
      :any -> nil
      :absent -> :absent
      :current -> elem(current, 1)
      :seen -> Enum.random([0 | model.seen])
    end
  end

  # -- The system under test -------------------------------------------------

  defp execute({:put, p, k, value, condition, deadline}, run, _context),
    do: KV.put(run <> p, k, value, opts(condition, deadline))

  defp execute({:delete, p, k, condition, deadline}, run, _context),
    do: KV.delete(run <> p, k, opts(condition, deadline))

  defp execute({:bad_option, :put, p, k}, run, _context) do
    assert_raise ArgumentError, fn -> KV.put(run <> p, k, "bad", if_vesion: :absent) end
    :bad_option
  end

  defp execute({:bad_option, :delete, p, k}, run, _context) do
    assert_raise ArgumentError, fn -> KV.delete(run <> p, k, if_vesion: 0) end
    :bad_option
  end

  defp execute({:put_nil_version, p, k}, run, _context),
    do: KV.put(run <> p, k, "value", if_version: nil)

  defp execute({:invalid_control, p, k}, run, _context) do
    assert_raise ArgumentError, fn -> KV.put(run <> p, k, "value", timeout: -1) end
    :invalid_control
  end

  defp execute({:get, p, k, consistency}, run, _context),
    do: KV.get(run <> p, k, consistency: consistency)

  defp execute({:scan, p, prefix, limit, with_versions}, run, _context) do
    opts =
      if prefix == "",
        do: [limit: limit, with_versions: with_versions],
        else: [prefix: prefix, limit: limit, with_versions: with_versions]

    scan_all(run <> p, opts, [])
  end

  # Replaces the partition writer through its supervisor, which waits for
  # the new one to start (and does not count it against the restart limit).
  defp execute({:restart_partition_writer, p}, run, _context) do
    {supervisor, id} = partition_writer_child(run <> p)
    :ok = Supervisor.terminate_child(supervisor, id)
    {:ok, _} = Supervisor.restart_child(supervisor, id)
    :ok
  end

  defp execute({:restart}, _run, context) do
    stop_supervised!(KV.Cluster)
    start_supervised!({KV.Cluster, context.cluster_opts})
    :ok
  end

  defp opts(condition, deadline) do
    if(condition, do: [if_version: condition], else: []) ++
      case deadline do
        nil -> []
        :later -> [deadline: System.os_time(:millisecond) + 60_000]
        :passed -> [deadline: System.os_time(:millisecond) - 1]
      end
  end

  defp scan_all(partition, opts, acc) do
    case KV.scan(partition, opts) do
      {:ok, %{rows: rows, cursor: nil}} ->
        {:ok, acc ++ rows}

      {:ok, %{rows: rows, cursor: cursor}} ->
        assert length(rows) == opts[:limit]
        scan_all(partition, Keyword.put(opts, :cursor, cursor), acc ++ rows)

      error ->
        error
    end
  end

  # -- The model --------------------------------------------------------------

  # The expected result of `command`, given the actual one where the model
  # cannot know the value (a new version), and the model after it.
  defp expect(model, {:bad_option, _kind, _p, _k}, _actual),
    do: {:bad_option, model}

  defp expect(model, {:put_nil_version, _p, _k}, _actual),
    do: {{:error, {:bad_request, :invalid_version}}, model}

  defp expect(model, {:invalid_control, _p, _k}, _actual),
    do: {:invalid_control, model}

  defp expect(model, {:put, _p, _k, _value, _condition, :passed}, _actual),
    do: {{:error, :deadline_exceeded}, model}

  defp expect(model, {:delete, _p, _k, _condition, :passed}, _actual),
    do: {{:error, :deadline_exceeded}, model}

  defp expect(model, {:put, p, k, value, condition, _deadline}, actual) do
    current = model.rows[{p, k}]

    if holds?(current, condition),
      do: written(model, p, k, value, actual),
      else: {{:error, {:conflict, version_of(current)}}, model}
  end

  defp expect(model, {:delete, p, k, condition, _deadline}, _actual) do
    current = model.rows[{p, k}]

    if holds?(current, condition),
      do: {:ok, %{model | rows: Map.delete(model.rows, {p, k})}},
      else: {{:error, {:conflict, version_of(current)}}, model}
  end

  defp expect(model, {:get, p, k, _consistency}, _actual) do
    case model.rows[{p, k}] do
      nil -> {{:ok, nil}, model}
      {value, version} -> {{:ok, %{value: value, version: version}}, model}
    end
  end

  defp expect(model, {:scan, p, prefix, _limit, with_versions}, _actual) do
    rows =
      for {{^p, k}, {value, version}} <- model.rows,
          String.starts_with?(k, prefix),
          do: if(with_versions, do: {k, value, version}, else: {k, value})

    {{:ok, Enum.sort(rows)}, model}
  end

  defp expect(model, {:restart_partition_writer, _}, _actual), do: {:ok, model}
  defp expect(model, {:restart}, _actual), do: {:ok, model}

  defp written(model, p, k, value, {:ok, version} = actual) when is_integer(version) do
    if version > Map.get(model.max_version, p, -1),
      do: {actual, record(model, p, k, {value, version})},
      else: {{:ok, :a_version_higher_than_any_before}, model}
  end

  defp written(model, _p, _k, _value, _actual), do: {{:ok, :a_new_version}, model}

  defp holds?(_current, nil), do: true
  defp holds?(nil, :absent), do: true
  defp holds?({_, version}, version), do: true
  defp holds?(_current, _condition), do: false

  defp version_of(nil), do: nil
  defp version_of({_, version}), do: version

  defp record(model, p, k, {_, version} = row) do
    %{
      model
      | rows: Map.put(model.rows, {p, k}, row),
        max_version: Map.put(model.max_version, p, version),
        seen: [version | model.seen]
    }
  end
end
