defmodule Slap.Inspect.Log do
  @moduledoc false

  alias Slap.Inspect.{Messages, Operation, Remote}

  # Component pages keep the catalog request's byte limits. Metadata is read
  # whole, because a component is identified by its decoded sid; its size is
  # counted in the page's bound (Records.log_catalog/3).

  def describe(connection, request, page, deadline) do
    paths = page.paths

    components =
      Map.new([:control, :index, :updates], fn component ->
        path = Map.fetch!(paths, component)

        row =
          Enum.find(page.rows, &match?(%{decoded: {:ok, %{path: ^path, value: %{sid: _}}}}, &1))

        {component, row || lookup(connection, request, path, deadline)}
      end)

    control = component(connection, request, components.control, :control, deadline)
    index = component(connection, request, components.index, :index, deadline)
    current = current(index)

    %{
      recognition: recognition(control, index, components),
      control: control,
      index: index,
      current: current,
      stored_snapshots: snapshots(page.rows, current),
      components: components,
      completeness: if(page.cursor, do: :partial_listing, else: :complete_listing)
    }
  end

  defp lookup(connection, request, path, deadline) do
    lookup = %{request | opts: [path: path]}

    case Remote.read(connection, lookup, {Slap.Streams.Introspect, :stream}, deadline) do
      {:ok, %{row: row}} -> row
      error -> error
    end
  end

  defp component(_connection, _request, nil, _kind, _deadline), do: {:error, :not_observed}

  defp component(connection, request, %{decoded: {:ok, %{value: %{sid: sid}}}}, kind, deadline) do
    opts =
      [sid: sid, component: kind, base: Keyword.fetch!(request.opts, :base), limit: 100] ++
        Keyword.take(request.opts, Operation.read_limits())

    with {:ok, page} <-
           Remote.read(
             connection,
             %{request | opts: opts},
             {Slap.SnapshotLog.Introspect, :messages},
             deadline
           ),
         {:ok, messages} <- Messages.assemble(page),
         do: {:ok, Map.put(page, :messages, messages)}
  end

  defp component(_connection, _request, _row, _kind, _deadline),
    do: {:error, :unreadable_component}

  defp recognition({:ok, page}, _index, _components) do
    case last_interpretation(page) do
      {:ok, %{state: state}} -> %{kind: :snapshot_log, state: state}
      _ -> %{kind: :unresolved, diagnostic: :control_not_validated}
    end
  end

  defp recognition({:error, :not_observed}, {:ok, page}, %{updates: updates})
       when not is_nil(updates) do
    valid = Enum.any?(page.rows, &match?(%{interpretation: {:ok, %{offset: _}}}, &1))

    case {valid, page.cursor} do
      {true, nil} -> %{kind: :snapshot_log, legacy: true}
      {true, _cursor} -> %{kind: :snapshot_log, legacy: true, index: :partial}
      {false, _cursor} -> %{kind: :unresolved, diagnostic: :legacy_index_not_validated}
    end
  end

  defp recognition(_control, _index, _components),
    do: %{kind: :unresolved, diagnostic: :components_not_validated}

  defp last_interpretation(%{cursor: nil, rows: rows}),
    do: rows |> Enum.reverse() |> Enum.find_value(& &1[:interpretation])

  defp last_interpretation(_page), do: {:error, :partial_page}

  defp current({:ok, %{current: {:ok, entry}}}), do: entry
  defp current({:ok, %{current: {:error, :deleted}}}), do: %{deleted: true}
  defp current({:ok, %{current: {:error, reason}}}), do: %{diagnostic: reason}
  defp current(_index), do: %{diagnostic: :index_incomplete}

  defp snapshots(rows, current) do
    for %{decoded: {:ok, %{path: path, component: %{component: :snapshot, offset: offset}}}} <-
          rows do
      %{path: path, offset: offset, selection: selection(current, offset)}
    end
  end

  defp selection(%{offset: offset}, offset), do: :current

  defp selection(%{retained: retained}, offset) do
    if Enum.any?(retained, &(&1.offset == offset)), do: :retained, else: :not_selected
  end

  defp selection(nil, _offset), do: :not_selected
  defp selection(%{deleted: true}, _offset), do: :not_selected
  defp selection(_current, _offset), do: :unknown
end
