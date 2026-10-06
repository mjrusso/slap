defmodule JepsenNode.DocServer do
  @moduledoc "The document servers, with a call that returns the document's state."

  use Slap.Yjs.DocServer

  @impl Yex.DocServer
  def handle_call(:state, _from, state),
    do: {:reply, Yex.encode_state_as_update!(state.doc), state}
end
