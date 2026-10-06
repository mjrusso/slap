defmodule Slap.Yjs.Test.Client do
  @moduledoc false
  # A simulated Yjs client: its own document, joined to the document's
  # server on its node through Slap.Yjs.Docs. It inserts unique tokens
  # ("<name>.<n> ") at token boundaries, sends its edits as sync messages,
  # applies the updates the server broadcasts, and sets its awareness state,
  # renewing it every second (y-protocols' clients renew every 15 s).
  # When the server goes down, it joins again and resynchronises: it takes
  # the server's state and sends its own (which covers anything the old
  # server lost), and sends its awareness state again.

  alias Slap.Yjs
  alias Slap.Yjs.Test.Server

  @join_opts [compact_bytes: 1024, idle_timeout: 60_000]

  def start(node, doc_id, name), do: Node.spawn(node, __MODULE__, :init, [doc_id, name])

  def edit(client), do: send(client, :edit)

  @doc "The client's text."
  def text(client), do: ask(client, :text)

  @doc "The tokens the client inserted, in order."
  def tokens(client), do: ask(client, :tokens)

  @doc "The client's awareness client id."
  def awareness_id(client), do: ask(client, :awareness_id)

  @doc """
  Makes the client's server store everything it has (`Slap.Yjs.DocServer.sync/2`,
  asked by the client, so after its own messages), then returns the tokens
  inserted so far: they must survive even if the client dies. `:error` if
  the server went down meanwhile.
  """
  def checkpoint(client), do: ask(client, :checkpoint, 30_000)

  defp ask(client, question, timeout \\ 5_000) do
    ref = make_ref()
    send(client, {question, self(), ref})

    receive do
      {^ref, answer} -> answer
    after
      timeout -> :timeout
    end
  end

  @doc false
  def init(doc_id, name) do
    doc = Yex.Doc.new()
    {:ok, _} = Yex.Doc.monitor_update(doc)
    {:ok, awareness} = Yex.Awareness.new(doc)
    :ok = Yex.Awareness.set_local_state(awareness, %{"client" => name})

    state = %{
      doc_id: doc_id,
      name: name,
      doc: doc,
      awareness: awareness,
      server: nil,
      tokens: []
    }

    send(self(), :renew)
    loop(join(state))
  end

  defp send_presence(state) do
    {:ok, presence} = Yex.Awareness.encode_update(state.awareness)
    Server.process_message_v1(state.server, <<1>> <> Yjs.Frame.frame(presence), self())
    state
  end

  defp join(state) do
    with {:ok, server} <- Yjs.Docs.join(Server, state.doc_id, @join_opts),
         ref = Process.monitor(server),
         {:ok, remote} <- remote_state(server, ref) do
      Yex.Doc.transaction(state.doc, :remote, fn -> :ok = Yex.apply_update(state.doc, remote) end)
      send_update(server, Yex.encode_state_as_update!(state.doc))
      send_presence(%{state | server: server})
    else
      _ ->
        receive after: (100 -> :ok)
        join(state)
    end
  end

  defp remote_state(server, ref) do
    server_doc = GenServer.call(server, :doc)
    {:ok, Yex.encode_state_as_update!(server_doc)}
  catch
    :exit, reason ->
      Process.demonitor(ref, [:flush])
      {:error, reason}
  end

  defp send_update(server, update),
    do: Server.process_message_v1(server, <<0, 2>> <> Yjs.Frame.frame(update), self())

  defp loop(state) do
    receive do
      :edit ->
        loop(insert(state))

      # A renewal bumps the state's clock, so servers that dropped it (when
      # the server it came through left their :pg group) take it again.
      :renew ->
        :ok = Yex.Awareness.set_local_state(state.awareness, %{"client" => state.name})
        Process.send_after(self(), :renew, 1_000)
        loop(send_presence(state))

      {:update_v1, update, nil, _doc} ->
        send_update(state.server, update)
        loop(state)

      {:update_v1, _update, :remote, _doc} ->
        loop(state)

      {:slap_yjs_update, _doc_id, update} ->
        Yex.Doc.transaction(state.doc, :remote, fn ->
          :ok = Yex.apply_update(state.doc, update)
        end)

        loop(state)

      {:DOWN, _ref, :process, server, _reason} when server == state.server ->
        loop(join(%{state | server: nil}))

      {:text, from, ref} ->
        send(from, {ref, current_text(state)})
        loop(state)

      {:tokens, from, ref} ->
        send(from, {ref, Enum.reverse(state.tokens)})
        loop(state)

      {:awareness_id, from, ref} ->
        send(from, {ref, Yex.Awareness.client_id(state.awareness)})
        loop(state)

      {:checkpoint, from, ref} ->
        answer =
          case Yjs.DocServer.sync(state.server, 20_000) do
            :ok -> {:ok, Enum.reverse(state.tokens)}
            {:error, _} -> :error
          end

        send(from, {ref, answer})
        loop(state)

      _other ->
        loop(state)
    end
  end

  # A new token at a random token boundary, so that tokens stay whole.
  defp insert(state) do
    token = "#{state.name}.#{length(state.tokens)}"
    text = current_text(state)
    boundaries = [0 | for({i, _} <- :binary.matches(text, " "), do: i + 1)]
    Yex.Text.insert(Yex.Doc.get_text(state.doc, "text"), Enum.random(boundaries), token <> " ")
    %{state | tokens: [token | state.tokens]}
  end

  defp current_text(state), do: state.doc |> Yex.Doc.get_text("text") |> Yex.Text.to_string()
end
