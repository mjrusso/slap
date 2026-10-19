defmodule Slap.Yjs.Presence do
  @moduledoc """
  Publishes an awareness state for a server-side participant, such as an
  agent or tool. The process subscribes to a document and keeps one client id
  for its lifetime.

  Awareness is Yjs's term for ephemeral participant state. To inspect and
  authorize client awareness messages, use the `Slap.Yjs.Awareness` codec.
  The process claims its id before publishing. Local client claims cannot
  take it over; remote awareness conflicts remain possible.

      {:ok, presence} =
        Slap.Yjs.Presence.start_link(
          module: MyApp.DocServer,
          doc_id: {"my-service", "doc-1"},
          state: %{"user" => %{"name" => "Assistant"}}
        )

      :ok =
        Slap.Yjs.Presence.update(presence, %{
          "user" => %{"name" => "Assistant"},
          "busy" => true
        })

  The process republishes its state every `:renew_every` milliseconds
  (15,000 by default). y-protocols clients remove a state if its clock does
  not advance for 30 seconds. If the document server stops, the process
  rejoins and republishes. Transient rejoin failures are retried after 1 s,
  backing off to at most 30 s. Updates received while disconnected are
  published on rejoin. Stopping the presence process removes its state.
  Deleting the document stops the process without a supervisor restart.

  Options:

    * `:module` (required) - the document server module, as for
      `Slap.Yjs.Docs.join/3`.
    * `:doc_id` (required) - the document.
    * `:state` (required) - a JSON map with string keys, or `fn client_id -> map end`.
      The function runs in the presence process once during startup; rejoining
      preserves the id and latest state. Nested maps require string keys;
      strings must be valid UTF-8, and integers must fit in signed 64 bits.
      Invalid maps raise `ArgumentError` in the caller. Invalid function results
      fail startup.
    * `:renew_every` - milliseconds between renewals (default 15,000).
    * `:docs`, `:timeout` - as for `Slap.Yjs.Docs.join/3`.
    * `:name` - the process name.
  """

  use GenServer, restart: :transient

  alias Slap.Yjs
  import Slap.Yjs.Follower, only: [transient?: 1]

  @retry_base 1_000
  @retry_max 30_000

  @doc "Starts a linked presence process and publishes its initial state."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")

    Keyword.validate!(opts, [:module, :doc_id, :state, :docs, :timeout, :renew_every, :name])

    for key <- [:module, :doc_id, :state],
        not Keyword.has_key?(opts, key),
        do: raise(ArgumentError, "#{inspect(key)} is required")

    unless is_function(opts[:state], 1), do: validate_state!(opts[:state])

    renew_every = Keyword.get(opts, :renew_every, 15_000)

    unless is_integer(renew_every) and renew_every > 0,
      do: raise(ArgumentError, ":renew_every must be a positive integer")

    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Sets `state`, publishing it now if connected, or on rejoin."
  @spec update(GenServer.server(), map(), timeout()) :: :ok | {:error, term()}
  def update(presence, state, timeout \\ 5_000)

  def update(presence, state, timeout) when is_map(state) do
    validate_state!(state)
    GenServer.call(presence, {:update, state}, timeout)
  end

  def update(_presence, _state, _timeout), do: raise(ArgumentError, "state must be a map")

  defp validate_state!(state) when is_map(state) do
    unless json?(state),
      do:
        raise(
          ArgumentError,
          "state must contain string keys and JSON values with UTF-8 strings and signed 64-bit integers"
        )
  end

  defp validate_state!(_state), do: raise(ArgumentError, ":state must be a map")

  defp json?(value) when is_map(value) do
    Enum.all?(value, fn {key, value} ->
      is_binary(key) and String.valid?(key) and json?(value)
    end)
  end

  defp json?([]), do: true
  defp json?([value | rest]), do: is_list(rest) and json?(value) and json?(rest)
  defp json?(value) when is_binary(value), do: String.valid?(value)

  defp json?(value) when is_integer(value),
    do: value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  defp json?(value) when is_float(value) or value in [nil, true, false], do: true
  defp json?(_value), do: false

  @impl true
  def init(opts) do
    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    state = initial_state(Keyword.fetch!(opts, :state), Yex.Awareness.client_id(awareness))
    :ok = Yex.Awareness.set_local_state(awareness, state)

    p = %{
      module: Keyword.fetch!(opts, :module),
      doc_id: Keyword.fetch!(opts, :doc_id),
      join_opts: Keyword.take(opts, [:docs, :timeout]),
      renew_every: Keyword.get(opts, :renew_every, 15_000),
      retry_delay: @retry_base,
      awareness: awareness,
      server: nil
    }

    case join(p) do
      {:ok, p} ->
        Process.send_after(self(), :renew, p.renew_every)
        {:ok, p}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp initial_state(fun, client_id) when is_function(fun, 1) do
    state = fun.(client_id)
    validate_state!(state)
    state
  end

  defp initial_state(state, _client_id), do: state

  @impl true
  def handle_call({:update, state}, _from, p) do
    :ok = Yex.Awareness.set_local_state(p.awareness, state)

    case publish(p) do
      :ok -> {:reply, :ok, p}
      {:error, reason} -> {:stop, reason, {:error, reason}, p}
    end
  end

  @impl true
  def handle_info(:renew, p) do
    # y-protocols treats a state as renewed only when its clock advances.
    :ok = Yex.Awareness.set_local_state(p.awareness, Yex.Awareness.get_local_state(p.awareness))

    case publish(p) do
      :ok ->
        Process.send_after(self(), :renew, p.renew_every)
        {:noreply, p}

      {:error, reason} ->
        {:stop, reason, p}
    end
  end

  def handle_info({:DOWN, _ref, :process, server, {:shutdown, :deleted}}, %{server: server} = p),
    do: {:stop, {:shutdown, :deleted}, p}

  def handle_info({:DOWN, _ref, :process, server, _reason}, %{server: server} = p) do
    # Peers increment the clock when removing a stopped server's clients.
    # A rejoin must advance twice to supersede that removal.
    state = Yex.Awareness.get_local_state(p.awareness)
    :ok = Yex.Awareness.clean_local_state(p.awareness)
    :ok = Yex.Awareness.set_local_state(p.awareness, state)

    rejoin(%{p | server: nil})
  end

  def handle_info(:rejoin, %{server: nil} = p), do: rejoin(p)

  # Docs.join/3 subscribes the caller, so document and awareness updates
  # arrive here even though Presence does not consume them.
  def handle_info(_message, p), do: {:noreply, p}

  defp rejoin(p) do
    case join(p) do
      {:ok, p} -> {:noreply, %{p | retry_delay: @retry_base}}
      {:error, :deleted} -> {:stop, {:shutdown, :deleted}, p}
      {:error, reason} when transient?(reason) -> retry(p)
      {:error, {:slap_yjs_load_failed, reason}} when transient?(reason) -> retry(p)
      {:error, reason} -> {:stop, reason, p}
    end
  end

  defp retry(p) do
    Process.send_after(self(), :rejoin, p.retry_delay)
    {:noreply, %{p | retry_delay: min(p.retry_delay * 2, @retry_max)}}
  end

  defp join(p) do
    with {:ok, server} <- Yjs.Docs.join(p.module, p.doc_id, p.join_opts),
         :ok <- Yjs.DocServer.claim_presence(server, Yex.Awareness.client_id(p.awareness), 5_000) do
      Process.monitor(server)
      p = %{p | server: server}

      with :ok <- publish(p), do: {:ok, p}
    else
      {:error, {:down, {:shutdown, :deleted}}} -> {:error, :deleted}
      {:error, {:down, _reason}} -> {:error, :unavailable}
      error -> error
    end
  end

  defp publish(%{server: nil}), do: :ok

  defp publish(p) do
    {:ok, update} = Yex.Awareness.encode_update(p.awareness)
    message = Yjs.DocServer.encode_message({:slap_yjs_awareness, p.doc_id, update})

    # The server monitor triggers rejoin on exit; renewal retries a timeout.
    case p.module.process_message_v1(p.server, message, self()) do
      {:error, {:down, _reason}} -> :ok
      {:error, :timeout} -> :ok
      result -> result
    end
  end
end
