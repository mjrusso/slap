defmodule Slap.Files.File do
  @moduledoc """
  A file's metadata, as `Slap.Files.get/1` and `Slap.Files.list/2` return
  it. `version` changes with every write of the file. `storage` is
  `:inline` (in the file's record) or `:object`.
  """

  @enforce_keys [:ref, :version, :size, :sha256, :content_type, :metadata, :storage]
  defstruct [:ref, :version, :size, :sha256, :content_type, :metadata, :storage]

  @type t :: %__MODULE__{
          ref: Slap.Files.ref(),
          version: non_neg_integer(),
          size: non_neg_integer(),
          sha256: binary(),
          content_type: String.t(),
          metadata: %{String.t() => String.t()},
          storage: :inline | :object
        }
end
