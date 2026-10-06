defmodule JepsenNode.Yjs do
  @moduledoc """
  The Yjs set workload. A document holds a Y.Array, "set". An add is made as
  a Yjs client would make it: join the document's server on this node, take
  its state, insert the element at a random index (so it depends on the
  elements around it), send the update, and wait until the server has
  stored it (`Slap.Yjs.DocServer.sync/2`).
  """

  alias JepsenNode.DocServer
  alias Slap.Yjs

  @timeout 10_000

  # Low, so that documents are compacted during a test.
  @compact_bytes 1_024

  @doc """
  Adds `element`. `{:error, :unavailable}` if the update was not sent, and
  `{:error, :indeterminate}` if it was sent but not confirmed stored (it may
  still be).
  """
  @spec add(String.t(), String.t()) :: :ok | {:error, :unavailable | :indeterminate}
  def add(doc, element) do
    # In a process of its own: the subscription and the server's messages
    # end with it.
    fn -> add_as_client({"jepsen", doc}, element) end
    |> Task.async()
    |> Task.await(:infinity)
  end

  defp add_as_client(doc_id, element) do
    with {:ok, server} <- Yjs.Docs.join(DocServer, doc_id, compact_bytes: @compact_bytes),
         {:ok, update} <- insert(server, element) do
      DocServer.process_message_v1(server, <<0, 2>> <> Yjs.Frame.frame(update), self())

      case Yjs.DocServer.sync(server, @timeout) do
        :ok -> :ok
        {:error, _} -> {:error, :indeterminate}
      end
    else
      {:error, _} -> {:error, :unavailable}
    end
  end

  # The update inserting `element`, from a document with the server's state.
  defp insert(server, element) do
    state = GenServer.call(server, :state, @timeout)
    doc = Yex.Doc.new()
    :ok = Yex.apply_update(doc, state)
    {:ok, before} = Yex.encode_state_vector(doc)
    set = Yex.Doc.get_array(doc, "set")
    Yex.Array.insert(set, :rand.uniform(Yex.Array.length(set) + 1) - 1, element)
    {:ok, Yex.encode_state_as_update!(doc, before)}
  catch
    :exit, reason -> {:error, reason}
  end

  @doc "The elements of the document as stored, loaded on this node."
  @spec read(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def read(doc) do
    with {:ok, loaded} <- Yjs.Store.load({"jepsen", doc}) do
      doc = Yex.Doc.new()

      for update <- List.wrap(loaded.snapshot) ++ loaded.updates,
          do: :ok = Yex.apply_update(doc, update)

      # As a document server does: yrs does not retry what a gap it skipped
      # held.
      {:ok, pending} = Yex.Doc.prune_pending(doc)
      if pending, do: :ok = Yex.apply_update(doc, pending)
      {:ok, doc |> Yex.Doc.get_array("set") |> Yex.Array.to_list()}
    end
  end
end
