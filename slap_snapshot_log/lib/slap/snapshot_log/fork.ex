defmodule Slap.SnapshotLog.Fork do
  @moduledoc false

  alias Slap.SnapshotLog.{Control, Store}
  alias Slap.Streams
  alias Slap.Streams.Offset

  # These leave the fork pending for a retry. `:busy` is a source still being
  # created.
  @ambiguous [:timeout, :unavailable, :overloaded, :busy]
  @races [:source_not_found, :source_gone, :not_found, :gone, :trimmed]

  @error_atoms Map.new(
                 [
                   :bad_offset,
                   :conflict,
                   :overloaded,
                   :stream_seq_conflict,
                   :timeout,
                   :trimmed,
                   :unavailable,
                   :busy,
                   :retries_exhausted,
                   :not_found,
                   :sealed,
                   :deleted,
                   :content_type_mismatch,
                   :gone,
                   :source_gone,
                   :source_not_found,
                   :payload_too_large,
                   :offset_beyond_tail,
                   :empty_array,
                   :empty_body,
                   :fork_of_itself,
                   :fork_offset_beyond_source,
                   :fork_offset_trimmed,
                   :max_fork_depth_exceeded,
                   :invalid_body,
                   :invalid_close,
                   :invalid_closed,
                   :invalid_content_type,
                   :invalid_expires_at,
                   :invalid_fork_offset,
                   :invalid_fork_source,
                   :invalid_fork_sub_offset,
                   :invalid_json,
                   :invalid_offset,
                   :invalid_path,
                   :invalid_producer,
                   :invalid_stream_seq,
                   :invalid_ttl,
                   :new_epoch_must_start_at_zero,
                   :trim_offset_beyond_tail,
                   :ttl_and_expires_at
                 ],
                 &{Atom.to_string(&1), &1}
               )

  def resume(target, control, deadline, hook \\ fn _ -> :ok end)

  def resume(_target, %{record: %{"status" => "ready", "capture" => capture}}, _deadline, _hook),
    do: {:ok, result(capture)}

  def resume(_target, %{record: %{"status" => "cancelled"} = record}, _deadline, _hook),
    do: {:error, decode_error(record["error"])}

  def resume(target, control, deadline, hook) do
    if Control.remaining(deadline) == 0 do
      {:error, :timeout}
    else
      advance(target, control, deadline, hook)
    end
  end

  defp advance(target, %{record: %{"stage" => "select"}} = control, deadline, hook) do
    hook.(:reservation)
    source = Store.new(control.record["source"], Store.stream_opts(target))

    with :ok <- Control.check(source, deadline),
         {:ok, view} <-
           Streams.metadata_fenced(Store.path(source, :updates), Control.opts(source, deadline)),
         true <- view.status == :active and not view.meta.closed,
         {:ok, state} <- Store.state(source, deadline),
         true <- state.deleted == nil,
         {:ok, snapshot} <- snapshot_identity(source, state.current, deadline) do
      record =
        Map.merge(control.record, %{
          "stage" => "updates",
          "snapshot" => snapshot,
          "requested_cutoff" => view.next_offset,
          "source_identity" =>
            Control.encode_identity(Control.identity(view, Store.path(source, :updates), source))
        })

      transition(target, control, record, deadline, hook, :selection)
    else
      false ->
        fail(target, control, :not_found, deadline)

      {:error, reason} when reason in [:deleted, :not_found, :gone] ->
        fail(target, control, :not_found, deadline)

      {:error, _} = error ->
        handle_failure(target, control, error, deadline, hook)
    end
  end

  defp advance(target, %{record: %{"stage" => "updates"}} = control, deadline, hook) do
    component(
      target,
      control,
      :updates,
      control.record["source"] <> "/.updates",
      false,
      deadline,
      hook
    )
  end

  defp advance(
         target,
         %{record: %{"stage" => "snapshot", "snapshot" => snapshot}} = control,
         deadline,
         hook
       ) do
    at = snapshot["offset"]
    source_path = Store.path(control.record["source"], {:snapshot, at})
    component(target, control, {:snapshot, at}, source_path, true, deadline, hook)
  end

  defp advance(target, %{record: %{"stage" => "fix"}} = control, deadline, hook) do
    updates = Enum.find(control.record["components"], &(&1["kind"] == "updates"))
    s = if control.record["snapshot"], do: control.record["snapshot"]["offset"], else: 0

    if updates["start"] <= s and s <= updates["cutoff"] do
      capture = %{
        "offset" => updates["cutoff"],
        "snapshot_offset" => if(control.record["snapshot"], do: s)
      }

      transition(
        target,
        control,
        Map.merge(control.record, %{"stage" => "index", "capture" => capture}),
        deadline,
        hook,
        :capture
      )
    else
      retry_candidate(target, control, deadline, hook)
    end
  end

  defp advance(target, %{record: %{"stage" => "index"}} = control, deadline, hook) do
    body = index_body(control.record["snapshot"], control.record["created_at"])
    path = Store.path(target, :index)

    with {:ok, _, _} <-
           Streams.create_guarded(
             path,
             Control.guard(control),
             [content_type: "application/json", body: body] ++ Control.opts(target, deadline)
           ),
         {:ok, view} <- Streams.metadata_fenced(path, Control.opts(target, deadline)) do
      entry = %{
        "kind" => "index",
        "identity" => Control.encode_identity(Control.identity(view, path, target))
      }

      record = %{
        control.record
        | "stage" => "publish",
          "components" => [entry | control.record["components"]]
      }

      transition(target, control, record, deadline, hook, :index)
    else
      error -> handle_failure(target, control, error, deadline, hook)
    end
  end

  defp advance(target, %{record: %{"stage" => "publish"}} = control, deadline, hook) do
    hook.(:before_publish)

    record =
      control.record
      |> Map.take(["kind", "source", "capture", "components"])
      |> Map.put("status", "ready")

    transition(target, control, record, deadline, hook, :publish)
  end

  defp advance(target, %{record: %{"stage" => "discard"}} = control, deadline, hook) do
    with :ok <- cleanup_components(target, control, deadline) do
      record =
        control.record
        |> Map.merge(%{
          "stage" => "select",
          "components" => [],
          "generation" => control.record["generation"] + 1
        })
        |> Map.delete("working")

      transition(target, control, record, deadline, hook)
    end
  end

  defp snapshot_identity(_source, nil, _deadline), do: {:ok, nil}

  defp snapshot_identity(source, snapshot, deadline) do
    path = Store.path(source, {:snapshot, snapshot.offset})

    with {:ok, view} <- Streams.metadata_fenced(path, Control.opts(source, deadline)),
         true <- view.status == :active do
      {:ok,
       %{
         "offset" => snapshot.offset,
         "cutoff" => view.next_offset,
         "identity" => Control.encode_identity(Control.identity(view, path, source))
       }}
    else
      false -> {:error, :candidate_changed}
      {:error, reason} when reason in [:gone, :not_found] -> {:error, :candidate_changed}
      error -> error
    end
  end

  defp component(target, control, kind, source_path, closed, deadline, hook) do
    path = Store.path(target, kind)

    cutoff =
      if kind == :updates,
        do: control.record["requested_cutoff"],
        else: control.record["snapshot"]["cutoff"]

    opts =
      [forked_from: source_path, fork_offset: cutoff, closed: closed] ++
        Control.opts(target, deadline)

    with {:ok, _, _} <- Streams.create_guarded(path, Control.guard(control), opts),
         {:ok, view} <- Streams.metadata_fenced(path, Control.opts(target, deadline)),
         %{capture: cap} <- view.meta.creation,
         true <- source_matches?(control.record, kind, cap) do
      entry = %{
        "kind" => if(kind == :updates, do: "updates", else: "snapshot"),
        "identity" => Control.encode_identity(Control.identity(view, path, target)),
        "source" => Control.encode_identity(cap.source),
        "start" => cap.start,
        "cutoff" => cap.offset
      }

      stage = if kind == :updates and control.record["snapshot"], do: "snapshot", else: "fix"

      record = %{
        control.record
        | "stage" => stage,
          "components" => [entry | control.record["components"]]
      }

      transition(
        target,
        control,
        record,
        deadline,
        hook,
        if(kind == :updates, do: :updates, else: :snapshot)
      )
    else
      false -> fail(target, control, :not_found, deadline)
      error -> handle_failure(target, control, error, deadline, hook)
    end
  end

  defp source_matches?(record, :updates, cap),
    do: Control.encode_identity(cap.source) == record["source_identity"]

  defp source_matches?(record, {:snapshot, _}, cap),
    do: Control.encode_identity(cap.source) == record["snapshot"]["identity"]

  defp transition(target, control, record, deadline, hook, step \\ nil) do
    case Control.put(target, control, record, deadline) do
      {:ok, next} ->
        if step, do: hook.(step)
        resume(target, next, deadline, hook)

      {:error, :conflict} ->
        reconcile(target, deadline, hook)

      {:error, :sealed} ->
        fail(target, control, :sealed, deadline)

      error ->
        error
    end
  end

  defp reconcile(target, deadline, hook) do
    with {:ok, control} <- Control.read(target, deadline),
         do: resume(target, control, deadline, hook)
  end

  defp handle_failure(target, control, {:error, reason} = error, deadline, hook)
       when reason in @ambiguous do
    case Control.read(target, deadline) do
      {:ok, %{offset: offset} = current} when offset != control.offset ->
        resume(target, current, deadline, hook)

      _ ->
        error
    end
  end

  defp handle_failure(target, _control, {:error, :conflict}, deadline, hook),
    do: reconcile(target, deadline, hook)

  defp handle_failure(
         target,
         control,
         {:error, {:bad_request, :fork_offset_trimmed}},
         deadline,
         hook
       ),
       do: retry_candidate(target, control, deadline, hook)

  defp handle_failure(target, control, {:error, :candidate_changed}, deadline, hook),
    do: retry_candidate(target, control, deadline, hook)

  defp handle_failure(target, control, {:error, reason}, deadline, hook) when reason in @races do
    if control.record["capture"] == nil,
      do: retry_candidate(target, control, deadline, hook),
      else: fail(target, control, reason, deadline)
  end

  defp handle_failure(target, control, {:error, reason}, deadline, _hook),
    do: fail(target, control, reason, deadline)

  defp retry_candidate(target, control, deadline, hook) do
    if control.record["generation"] < control.record["retries"] do
      record =
        control.record
        |> Map.put("stage", "discard")
        |> Map.put("working", working(target, control))

      transition(target, control, record, deadline, hook, :discard)
    else
      fail(target, control, :retries_exhausted, deadline)
    end
  end

  def cancel(target, control, reason, deadline, deleted \\ false)

  def cancel(
        _target,
        %{record: %{"status" => "cancelled", "deleted" => true}} = control,
        _reason,
        _deadline,
        true
      ),
      do: {:ok, control}

  def cancel(target, control, reason, deadline, deleted) do
    record =
      control.record
      |> Map.take(["kind", "source", "capture", "components"])
      |> Map.merge(%{
        "status" => "cancelled",
        "error" => encode_error(reason),
        "cleanup" => true,
        "deleted" => deleted
      })
      |> Map.put("working", working(target, control))

    Control.put(target, control, record, deadline, false)
  end

  defp fail(target, control, reason, deadline) do
    case cancel(target, control, reason, deadline) do
      {:ok, _cancelled} ->
        {:error, reason}

      {:error, :conflict} ->
        reconcile(target, deadline, fn _ -> :ok end)

      error ->
        error
    end
  end

  def cleanup(target, control, deadline) do
    with :ok <- seal_deleted(target, control, deadline),
         :ok <- cleanup_components(target, control, deadline),
         do: Control.finish_cleanup(target, control, deadline)
  end

  defp seal_deleted(target, %{record: %{"deleted" => true}}, deadline),
    do: Streams.seal(target.base, Control.opts(target, deadline))

  defp seal_deleted(_target, _control, _deadline), do: :ok

  defp cleanup_components(target, control, deadline) do
    known = Enum.map(control.record["components"] || [], & &1["identity"])

    with :ok <- Store.each_ok(known, &remove(target, Control.decode_identity(&1), deadline)),
         :ok <-
           Store.each_ok(
             List.wrap(control.record["working"]),
             &remove_working(target, control, &1, deadline)
           ),
         do: cleanup_deleted(target, control, deadline)
  end

  defp cleanup_deleted(target, %{record: %{"deleted" => true}}, deadline) do
    bounded = %{target | timeout: Control.remaining(deadline)}

    with {:ok, stored} <- Store.stored(bounded),
         do:
           Store.each_ok(
             stored,
             &Store.delete_snapshot(%{target | timeout: Control.remaining(deadline)}, &1)
           )
  end

  defp cleanup_deleted(_target, _control, _deadline), do: :ok

  defp working(target, control) do
    case control.record["stage"] do
      "updates" ->
        %{"path" => Store.path(target, :updates), "offset" => control.offset}

      "snapshot" ->
        %{
          "path" => Store.path(target, {:snapshot, control.record["snapshot"]["offset"]}),
          "offset" => control.offset
        }

      "index" ->
        %{"path" => Store.path(target, :index), "offset" => control.offset}

      _ ->
        control.record["working"]
    end
  end

  defp remove_working(target, control, working, deadline) do
    owner = %{identity: control.identity, offset: working["offset"]}
    Control.remove_component(target, working["path"], owner, deadline)
  end

  defp remove(target, identity, deadline) do
    case Streams.delete_identity(identity, Control.opts(target, deadline)) do
      :ok -> :ok
      {:error, reason} when reason in [:not_found, :gone, :conflict] -> :ok
      error -> error
    end
  end

  defp index_body(nil, _created_at), do: ""

  defp index_body(snapshot, created_at),
    do:
      JSON.encode!(%{
        "snapshotOffset" => Offset.encode(snapshot["offset"]),
        "createdAt" => created_at,
        "retained" => []
      })

  defp result(capture),
    do: %{offset: capture["offset"], snapshot_offset: capture["snapshot_offset"]}

  defp encode_error({:bad_request, reason}), do: %{"bad_request" => Atom.to_string(reason)}
  defp encode_error(reason) when is_atom(reason), do: Atom.to_string(reason)

  defp decode_error(%{"bad_request" => reason}),
    do: {:bad_request, Map.fetch!(@error_atoms, reason)}

  defp decode_error(reason), do: Map.fetch!(@error_atoms, reason)
end
