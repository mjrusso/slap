defmodule Slap.SlateDB.Subscription do
  @moduledoc """
  A subscription from `Slap.SlateDB.subscribe/3`. Its `:ref` is the
  reference in every message the subscription sends; match on it to tell
  subscriptions apart:

      {:ok, %Slap.SlateDB.Subscription{ref: ref}} = Slap.SlateDB.subscribe(db, :mine)
      receive do
        {:slap_slatedb_durable, ^ref, :mine, durable_seq} -> durable_seq
      end
  """

  @enforce_keys [:ref, :resource]
  defstruct [:ref, :resource]

  @type t :: %__MODULE__{ref: reference(), resource: reference()}
end
