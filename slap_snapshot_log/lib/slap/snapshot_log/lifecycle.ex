defmodule Slap.SnapshotLog.Lifecycle do
  @moduledoc false

  alias Slap.SnapshotLog.{Control, Fork, Initialization, Store}

  @spec ensure(String.t() | Store.t(), keyword()) :: :ok | {:error, term()}
  def ensure(base, opts \\ []) do
    store = if is_binary(base), do: Store.new(base, opts), else: base

    deadline =
      Keyword.get_lazy(opts, :deadline, fn ->
        Control.deadline(Keyword.get(opts, :timeout, store.timeout || 30_000))
      end)

    initialize(store, deadline)
  end

  def live(base, deadline \\ nil) do
    store = store(base)
    deadline = deadline || Control.deadline(store.timeout || 30_000)
    with :ok <- Control.check(store, deadline), do: Store.live(store, deadline)
  end

  def fork(source, target, opts) do
    deadline = Control.deadline(Keyword.get(opts, :timeout, 30_000))
    target = Store.new(target, opts)

    request = %{
      "source" => source,
      "kind" => "fork",
      "status" => "pending",
      "stage" => "select",
      "generation" => 0,
      "retries" => Keyword.get(opts, :retries, 5),
      "components" => [],
      "created_at" => System.system_time(:millisecond)
    }

    with {:ok, control} <- reservation(target, request, deadline),
         do:
           Fork.resume(target, control, deadline, Keyword.get(opts, :after_step, fn _ -> :ok end))
  end

  defp reservation(target, request, deadline) do
    case Control.read(target, deadline) do
      {:ok, nil} ->
        absent = Enum.map([:updates, :index], &Store.path(target, &1))

        case Control.claim(target, request, absent, deadline) do
          {:error, :conflict} -> {:error, :exists}
          {:ok, _status, control} -> match_request(control, request)
          result -> result
        end

      {:ok, control} ->
        match_request(control, request)

      error ->
        error
    end
  end

  defp match_request(%{record: %{"deleted" => true}}, _request),
    do: {:error, :deleted}

  defp match_request(%{state: state, record: %{"source" => source}} = control, %{
         "source" => source
       })
       when state in [:forking, :forked, :fork_cancelled],
       do: {:ok, control}

  defp match_request(_control, _request), do: {:error, :exists}

  @spec delete(String.t() | Store.t(), keyword()) :: :ok | {:error, term()}
  def delete(base, opts \\ []) do
    store = if is_binary(base), do: Store.new(base, []), else: base
    deadline = Control.deadline(store.timeout || 30_000)

    cancel_creation(store, deadline, opts)
  end

  defp cancel_creation(store, deadline, opts) do
    case cancel_creation_attempt(store, deadline, opts) do
      {:error, :conflict} ->
        if Control.remaining(deadline) == 0,
          do: {:error, :timeout},
          else: cancel_creation(store, deadline, opts)

      result ->
        result
    end
  end

  defp cancel_creation_attempt(store, deadline, opts) do
    case Control.read(store, deadline) do
      {:ok, nil} ->
        record = %{"kind" => "ordinary", "status" => "cancelled", "deleted" => true}

        case Control.claim(store, record, [], deadline) do
          {:ok, _status, control} -> delete_control(store, control, deadline, opts)
          {:error, :sealed} -> Store.delete(store, opts)
          error -> error
        end

      {:ok, control} ->
        delete_control(store, control, deadline, opts)

      error ->
        error
    end
  end

  defp initialize(store, deadline) do
    case Control.read(store, deadline) do
      {:ok, nil} -> Initialization.claim(store, deadline)
      {:ok, control} -> continue(store, control, deadline)
      {:error, :sealed} -> {:error, :deleted}
      error -> error
    end
  end

  # The claimant may have stopped, so any caller finishes the initialization.
  defp continue(store, %{state: :initializing} = control, deadline),
    do: Initialization.initialize_claim(store, control, deadline)

  defp continue(_store, control, _deadline), do: Control.access(control)

  def tail(base) do
    store = store(base)

    with :ok <- Control.check(store, Control.deadline(store.timeout || 30_000)),
         do: Store.tail(store)
  end

  def history(base) do
    store = store(base)
    with :ok <- live(store), do: Store.history(store)
  end

  def recover(store, deadline) do
    with {:ok, control} <- Control.read(store, deadline),
         do: recover_control(store, control, deadline)
  end

  defp recover_control(_store, nil, _deadline), do: :ok

  defp recover_control(store, %{state: :initializing} = control, deadline),
    do: Initialization.initialize_claim(store, control, deadline)

  defp recover_control(store, %{state: :initialization_cancelled} = control, deadline),
    do: Initialization.cleanup_initialization(store, control, deadline)

  defp recover_control(store, %{state: :fork_cancelled} = control, deadline),
    do: Fork.cleanup(store, control, deadline)

  defp recover_control(store, %{state: :forking} = control, deadline),
    do: Fork.resume(store, control, deadline)

  defp recover_control(_store, _control, _deadline), do: :ok

  defp delete_control(store, %{state: state} = control, deadline, _opts)
       when state in [:forking, :forked, :fork_cancelled] do
    with {:ok, cancelled} <- Fork.cancel(store, control, :deleted, deadline, true),
         do: Fork.cleanup(store, cancelled, deadline)
  end

  defp delete_control(store, %{state: state} = control, deadline, _opts)
       when state in [:initializing, :initialization_cancelled] do
    with {:ok, cancelled} <- Initialization.cancel_pending(store, control, deadline),
         do: Initialization.cleanup_initialization(store, cancelled, deadline)
  end

  defp delete_control(store, control, deadline, opts) do
    record = Map.merge(control.record, %{"status" => "cancelled", "deleted" => true})

    with {:ok, _} <- Control.put(store, control, record, deadline, false),
         do: Store.delete(store, opts)
  end

  defp store(%Store{} = store), do: store
  defp store(base), do: Store.new(base, [])
end
