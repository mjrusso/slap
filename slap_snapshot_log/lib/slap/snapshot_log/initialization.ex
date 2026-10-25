defmodule Slap.SnapshotLog.Initialization do
  @moduledoc false

  alias Slap.SnapshotLog.{Control, Store}
  alias Slap.Streams

  def claim(store, deadline) do
    case Control.claim(store, %{"kind" => "initialization", "status" => "pending"}, [], deadline) do
      {:ok, :created, control} -> initialize_claim(store, control, deadline)
      {:ok, :exists, _control} -> Control.check(store, deadline)
      {:error, :sealed} -> sealed_legacy(store, deadline)
      error -> error
    end
  end

  defp sealed_legacy(store, deadline) do
    with {:ok, %{status: :active, meta: %{closed: false}}} <-
           Streams.metadata(Store.path(store, :updates), Control.opts(store, deadline)),
         {:ok, %{status: :active}} <-
           Streams.metadata(Store.path(store, :index), Control.opts(store, deadline)),
         {:ok, control} <-
           Streams.internal(
             Control.path(store),
             {:owned_identity, nil},
             Control.opts(store, deadline)
           ) do
      if control == nil, do: :ok, else: Control.check(store, deadline)
    else
      {:error, reason} when reason in [:timeout, :unavailable] -> {:error, reason}
      _ -> {:error, :deleted}
    end
  end

  def initialize_claim(
        store,
        %{record: %{"kind" => "initialization", "status" => "pending"}} = control,
        deadline
      ) do
    with :ok <- ordinary_component(store, control, :updates, "application/octet-stream", deadline),
         :ok <- ordinary_component(store, control, :index, "application/json", deadline),
         {:ok, _} <-
           Control.put(store, control, %{"kind" => "ordinary", "status" => "ready"}, deadline) do
      :ok
    else
      {:error, :conflict} ->
        Control.check(store, deadline)

      {:error, reason} when reason in [:sealed, :deleted] ->
        cancel_initialization(store, control, deadline)

      error ->
        error
    end
  end

  def initialize_claim(store, _control, deadline), do: Control.check(store, deadline)

  defp cancel_initialization(store, control, deadline) do
    case cancel_pending(store, control, deadline, false) do
      {:ok, _} -> {:error, :deleted}
      {:error, :conflict} -> Control.check(store, deadline)
      error -> error
    end
  end

  def cancel_pending(store, control, deadline, deleted \\ true)

  def cancel_pending(
        _store,
        %{record: %{"status" => "cancelled", "deleted" => true}} = control,
        _deadline,
        true
      ),
      do: {:ok, control}

  def cancel_pending(store, control, deadline, deleted) do
    record = %{
      "kind" => "initialization",
      "status" => "cancelled",
      "deleted" => deleted,
      "cleanup" => true,
      "owner_offset" => control.offset
    }

    Control.put(store, control, record, deadline, false)
  end

  def cleanup_initialization(store, control, deadline) do
    with :ok <- cleanup_initialization_components(store, control, deadline),
         do: Control.finish_cleanup(store, control, deadline)
  end

  # A durable seal lets explicit deletion retire legacy components as well.
  defp cleanup_initialization_components(store, %{record: %{"deleted" => true}}, deadline) do
    with :ok <- Streams.seal(store.base, Control.opts(store, deadline)),
         {:ok, snapshots} <- Store.stored(%{store | timeout: Control.remaining(deadline)}),
         :ok <-
           Store.each_ok(
             snapshots,
             &remove_current(store, Store.path(store, {:snapshot, &1}), deadline)
           ),
         do:
           Store.each_ok(
             [:updates, :index],
             &remove_current(store, Store.path(store, &1), deadline)
           )
  end

  defp cleanup_initialization_components(store, control, deadline) do
    owner = %{identity: control.identity, offset: control.record["owner_offset"]}

    Store.each_ok(
      [:updates, :index],
      &Control.remove_component(store, Store.path(store, &1), owner, deadline)
    )
  end

  defp remove_current(store, path, deadline) do
    case Streams.metadata_fenced(path, Control.opts(store, deadline)) do
      {:ok, view} ->
        Streams.delete_identity(
          Control.identity(view, path, store),
          Control.opts(store, deadline)
        )

      {:error, :not_found} ->
        :ok

      error ->
        error
    end
  end

  defp ordinary_component(store, control, component, type, deadline) do
    case Streams.create_guarded(
           Store.path(store, component),
           Control.guard(control),
           [content_type: type] ++ Control.opts(store, deadline)
         ) do
      {:ok, _, _} -> :ok
      {:error, reason} when reason in [:conflict, :sealed] -> {:error, :deleted}
      error -> error
    end
  end
end
