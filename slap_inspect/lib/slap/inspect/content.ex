defmodule Slap.Inspect.Content do
  @moduledoc false

  alias Slap.Inspect.{Capture, Collection, Records, Remote, Request}

  @capture_attempts 3

  def execute(connection, request, deadline) do
    result = capture(connection, request, deadline, @capture_attempts)

    if System.monotonic_time(:millisecond) >= deadline,
      do: {:error, :deadline},
      else: result
  end

  defp capture(_connection, _request, _deadline, 0), do: {:error, :incomplete}

  defp capture(connection, request, deadline, attempts) do
    result =
      with {:ok, capture} <- Capture.prepare(connection, request, deadline) do
        if request.opts[:preflight],
          do: {:ok, Map.take(capture, [:owner, :encoded_bytes])},
          else: collect(connection, request, capture, deadline)
      end

    case result do
      {:error, :capture_changed} ->
        capture(connection, request, deadline, attempts - 1)

      error ->
        error
    end
  end

  defp collect(connection, request, capture, deadline) do
    with {:ok, {snapshot, snapshot_bytes}} <- snapshot(connection, request, capture, deadline),
         :ok <- boundary(connection, request, capture, deadline),
         {:ok, data} <- updates(connection, request, capture, snapshot_bytes, deadline),
         :ok <- coverage(data.messages, capture),
         {:ok, checked} <- Capture.prepare(connection, request, deadline) do
      if same_capture?(capture, checked) do
        apply_content(connection, request, capture, snapshot, data.messages, deadline)
      else
        {:error, :capture_changed}
      end
    else
      {:error, reason}
      when reason in [
             :stream_identity_changed,
             :stream_not_observed,
             :not_found_custom_placement_key_possible,
             :soft_deleted,
             :coverage_changed,
             :capture_changed
           ] ->
        {:error, :capture_changed}

      {:error, :incomplete_message_parts} = error ->
        recheck_partial(connection, request, capture, deadline, error)

      error ->
        error
    end
  end

  defp recheck_partial(connection, request, capture, deadline, error) do
    case Capture.prepare(connection, request, deadline) do
      {:ok, checked} ->
        if same_capture?(capture, checked),
          do: error,
          else: {:error, :capture_changed}

      result ->
        result
    end
  end

  defp same_capture?(before, later), do: before.identity == later.identity

  defp snapshot(_connection, _request, %{snapshot: nil}, _deadline), do: {:ok, {nil, 0}}

  defp snapshot(connection, request, capture, deadline) do
    stream = capture.snapshot

    with {:ok, data} <-
           Collection.read(connection, request, stream, 0, stream.tail, capture.limits, deadline),
         true <- Collection.retained_from(data.messages, stream.tail) == 0 do
      bytes = data.messages |> Enum.map(& &1.value) |> IO.iodata_to_binary()
      {:ok, {bytes, data.encoded_bytes}}
    else
      false -> {:error, :coverage_changed}
      error -> error
    end
  end

  defp updates(connection, request, capture, snapshot_bytes, deadline) do
    limits = %{
      capture.limits
      | input_bytes: capture.limits.input_bytes - snapshot_bytes
    }

    Collection.read(
      connection,
      request,
      capture.updates,
      capture.from,
      capture.until,
      limits,
      deadline
    )
  end

  defp coverage(messages, capture) do
    retained_from = Collection.retained_from(messages, capture.until)

    cond do
      capture.history and retained_from != 0 ->
        {:error, {:history_unavailable, retained_from}}

      retained_from != capture.from ->
        {:error, :coverage_changed}

      true ->
        :ok
    end
  end

  defp boundary(_connection, _request, %{history: false}, _deadline), do: :ok
  defp boundary(_connection, _request, %{until: tail, tail: tail}, _deadline), do: :ok

  defp boundary(connection, request, capture, deadline) do
    offset = capture.until

    opts =
      Keyword.merge(request.opts,
        path: capture.updates.path,
        expected_sid: capture.updates.sid,
        from: offset,
        until: offset + 1,
        limit: 1
      )

    with {:ok, logical} <- Request.derive(request, :logical, opts),
         {:ok, page} <- Records.page(connection, logical, deadline) do
      case page do
        %{
          fragment_contract: 1,
          fragments: [%{offset: ^offset, before: nil, status: status} | _]
        }
        when status in [:more, :complete] ->
          :ok

        _ ->
          {:error, :not_an_entry_boundary}
      end
    end
  end

  defp apply_content(connection, request, capture, snapshot, messages, deadline) do
    budget = deadline - System.monotonic_time(:millisecond)

    input = %{
      snapshot: snapshot,
      updates: Enum.map(messages, & &1.value),
      roots: request.opts[:roots] || [],
      limits: capture.limits
    }

    # Reconstruction reads nothing, so the entry node runs it.
    with true <- budget > 0,
         {:ok, reply} <-
           Remote.call(
             connection.entry,
             Slap.Yjs.Introspect,
             :reconstruct,
             [Remote.api_version(), budget, input],
             budget + 1000
           ),
         {:ok, result} <- reply do
      {:ok,
       Map.merge(result, %{
         owner: capture.owner,
         shard: request.shard,
         durable_seq: nil,
         coverage: %{
           from: capture.from,
           until: capture.until,
           snapshot_offset: capture.snapshot && capture.snapshot.offset,
           exact: capture.history
         },
         label: label(capture)
       })}
    else
      false -> {:error, :deadline}
      error -> error
    end
  end

  defp label(%{history: true, until: offset}),
    do: "History before offset #{offset}; reconstructed without a snapshot"

  defp label(capture),
    do:
      "Includes all updates before offset #{capture.until}; the snapshot may include later state"
end
