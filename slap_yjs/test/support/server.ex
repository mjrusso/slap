defmodule Slap.Yjs.Test.Server do
  @moduledoc false
  # A document server for tests, with calls to reach its document and its
  # awareness states and its assigns.
  use Slap.Yjs.DocServer, restart: :temporary

  @impl Yex.DocServer
  def handle_call(:doc, _from, state), do: {:reply, state.doc, state}

  def handle_call({:assign, key}, _from, state), do: {:reply, state.assigns[key], state}

  def handle_call(:awareness_ids, _from, state),
    do: {:reply, Enum.sort(Yex.Awareness.get_client_ids(state.awareness)), state}
end
