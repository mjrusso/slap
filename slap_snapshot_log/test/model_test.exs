defmodule Slap.SnapshotLog.ModelTest do
  use Slap.SnapshotLog.Test.ClusterCase, async: false
  use ExUnitProperties

  @moduletag :capture_log
  @moduletag timeout: :infinity

  property "branches reconstruct their captured prefix and their own entries" do
    command =
      tuple(
        {member_of([:append, :snapshot, :fork, :delete, :initialize]), integer(0..5),
         integer(0..5)}
      )

    check all(commands <- list_of(command, min_length: 20, max_length: 40), max_runs: 100) do
      prefix = base()
      paths = Map.new(0..5, &{&1, prefix <> "-#{&1}"})
      initial = [{:initialize, 0, 0}, {:append, 0, 0}, {:fork, 0, 1}, {:append, 1, 0}]

      Enum.reduce(Enum.with_index(initial ++ commands), %{}, fn {command, n}, model ->
        model = apply_command(command, n, paths, model)
        assert_logs(paths, model)
        model
      end)

      for path <- Map.values(paths), do: :ok = SnapshotLog.delete(path)
    end
  end

  defp apply_command({:initialize, key, _}, _n, paths, model) do
    case model[key] do
      nil ->
        assert {:reset, %{snapshot: nil}} = SnapshotLog.next(paths[key], nil)
        Map.put(model, key, ordinary())

      %{status: :live} ->
        assert {:reset, _} = SnapshotLog.next(paths[key], nil)
        model

      _ ->
        assert {:error, :deleted} = SnapshotLog.next(paths[key], nil)
        model
    end
  end

  defp apply_command({:append, key, _}, n, paths, model) do
    case model[key] do
      %{status: status} when status != :live ->
        assert {:error, :deleted} = SnapshotLog.append(paths[key], Integer.to_string(n))
        model

      log ->
        log = log || ordinary()
        entry = Integer.to_string(n)
        log = %{log | entries: [entry | log.entries], tail: log.tail + 4 + byte_size(entry)}
        assert {:ok, tail} = SnapshotLog.append(paths[key], entry)
        assert tail == log.tail
        Map.put(model, key, log)
    end
  end

  defp apply_command({:snapshot, key, _}, _n, paths, model) do
    case model[key] do
      %{status: :live} = log ->
        assert :ok =
                 SnapshotLog.snapshot(
                   paths[key],
                   log.tail,
                   JSON.encode!(Enum.reverse(log.entries))
                 )

        Map.put(model, key, %{log | snapshot_offset: log.tail})

      nil ->
        assert {:error, :not_found} = SnapshotLog.snapshot(paths[key], 0, "[]")
        model

      _ ->
        assert {:error, :deleted} = SnapshotLog.snapshot(paths[key], 0, "[]")
        model
    end
  end

  defp apply_command({:delete, key, _}, _n, paths, model) do
    assert :ok = SnapshotLog.delete(paths[key])
    Map.put(model, key, %{status: :deleted})
  end

  defp apply_command({:fork, source, source}, _n, paths, model) do
    assert {:error, {:bad_request, :same_source_and_target}} =
             SnapshotLog.fork(paths[source], paths[source])

    model
  end

  defp apply_command({:fork, source, target}, _n, paths, model) do
    case {model[source], model[target]} do
      {_, %{status: :deleted}} ->
        assert {:error, :deleted} = SnapshotLog.fork(paths[source], paths[target])
        model

      {_, %{source: ^source, capture: capture}} when capture != nil ->
        assert {:ok, ^capture} = SnapshotLog.fork(paths[source], paths[target])
        model

      {_, %{source: ^source, error: error}} ->
        assert {:error, ^error} = SnapshotLog.fork(paths[source], paths[target])
        model

      {_, target_log} when target_log != nil ->
        assert {:error, :exists} = SnapshotLog.fork(paths[source], paths[target])
        model

      {%{status: :live} = source_log, nil} ->
        capture = %{offset: source_log.tail, snapshot_offset: source_log.snapshot_offset}
        assert {:ok, ^capture} = SnapshotLog.fork(paths[source], paths[target])
        Map.put(model, target, %{source_log | source: source, capture: capture})

      {_, nil} ->
        assert {:error, :not_found} = SnapshotLog.fork(paths[source], paths[target])
        Map.put(model, target, %{status: :cancelled, source: source, error: :not_found})
    end
  end

  defp ordinary,
    do: %{
      status: :live,
      entries: [],
      tail: 0,
      snapshot_offset: nil,
      source: nil,
      capture: nil
    }

  defp assert_logs(paths, model) do
    for {key, log} <- model, do: assert_log(paths[key], log)
  end

  defp assert_log(path, %{status: :live} = log) do
    {snapshot, entries, tail} = load(path)
    prefix = if snapshot, do: JSON.decode!(snapshot), else: []
    assert prefix ++ entries == Enum.reverse(log.entries)
    assert tail == log.tail
  end

  defp assert_log(path, _log), do: assert({:error, :deleted} = SnapshotLog.next(path, nil))
end
