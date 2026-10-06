defmodule Slap.SlateDB.Error do
  @moduledoc """
  An error returned by SlateDB.

  `kind` is one of:

    * `:conflict` - a transaction conflict. Retry the transaction or give up.
    * `:closed` - the database is closed. `reason` is `:clean`, `:fenced`
      (another writer opened the same database), `:panic` or `:unknown`.
    * `:unavailable` - object storage or the network is unavailable. Retry
      later.
    * `:invalid` - the request was not valid, for example an empty key.
    * `:data` - stored data is corrupt or cannot be read.
    * `:internal` - an unexpected error inside SlateDB or this binding.
    * `:timeout` - no reply within the call's `:timeout`. The operation was not
      cancelled and may still complete.
  """

  @type kind ::
          :conflict | :closed | :unavailable | :invalid | :data | :internal | :timeout

  @type t :: %__MODULE__{
          kind: kind(),
          reason: :clean | :fenced | :panic | :unknown | nil,
          message: String.t()
        }

  defexception [:kind, :reason, :message]

  @impl true
  def message(%__MODULE__{kind: kind, message: message}), do: "(#{kind}) #{message}"
end
