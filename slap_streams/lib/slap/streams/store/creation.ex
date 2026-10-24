defmodule Slap.Streams.Store.Identity do
  @moduledoc false
  @enforce_keys [:path, :shard, :sid]
  defstruct @enforce_keys
  def new(ctx, path, sid), do: %__MODULE__{path: path, shard: ctx.n, sid: sid}

  @type t :: %__MODULE__{path: binary(), shard: non_neg_integer(), sid: pos_integer()}
end

defmodule Slap.Streams.Store.Capture do
  @moduledoc false
  alias Slap.Streams.Store.Identity

  @enforce_keys [:source, :target, :content_type, :offset, :start, :closed, :depth]
  defstruct @enforce_keys ++ [:ttl_s, :expires_at_ms, :prefix]

  @type t :: %__MODULE__{
          source: Identity.t(),
          target: Identity.t(),
          content_type: binary(),
          offset: non_neg_integer(),
          start: non_neg_integer(),
          closed: boolean(),
          depth: non_neg_integer(),
          ttl_s: non_neg_integer() | nil,
          expires_at_ms: integer() | nil,
          prefix: binary() | nil
        }
end

defmodule Slap.Streams.Store.Creation do
  @moduledoc false

  @type request :: %{
          forked_from: binary(),
          source_placement_key: binary(),
          fork_offset: non_neg_integer() | nil,
          fork_sub_offset: non_neg_integer(),
          content_type: binary() | nil,
          ttl_s: non_neg_integer() | nil,
          expires_at_ms: integer() | nil,
          closed: boolean(),
          body: binary()
        }

  @type pending ::
          Slap.Streams.Store.ReferenceCreation.t() | Slap.Streams.Store.CopyCreation.t()
  @type t ::
          pending()
          | Slap.Streams.Store.PublishedReference.t()
          | Slap.Streams.Store.PublishedCopy.t()
end

defmodule Slap.Streams.Store.ReferenceCreation do
  @moduledoc false
  alias Slap.Streams.Store.{Creation, Identity}

  @enforce_keys [:source, :target, :request]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          source: Identity.t(),
          target: Identity.t(),
          request: Creation.request()
        }
end

defmodule Slap.Streams.Store.CopyProgress do
  @moduledoc false
  alias Slap.Streams.Store.Capture

  # Repair measures `:creation_grace` from `progressed_ms`: every write that
  # advances the copy must set it.
  @enforce_keys [:capture, :cursor, :progressed_ms]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          capture: Capture.t(),
          cursor: non_neg_integer(),
          progressed_ms: integer()
        }
end

defmodule Slap.Streams.Store.CopyCreation do
  @moduledoc false
  alias Slap.Streams.Store.{CopyProgress, Creation, Identity}

  @enforce_keys [:source, :target, :request]
  defstruct [:progress | @enforce_keys]

  @type t :: %__MODULE__{
          source: Identity.t(),
          target: Identity.t(),
          request: Creation.request(),
          progress: CopyProgress.t() | nil
        }
end

defmodule Slap.Streams.Store.Selection do
  @moduledoc false
  alias Slap.Streams.Store.Identity

  @enforce_keys [:kind, :source, :target]
  defstruct @enforce_keys ++ [:fork_offset, :content_type, fork_sub_offset: 0]

  @type t :: %__MODULE__{
          kind: :reference | :copy,
          source: Identity.t(),
          target: Identity.t(),
          fork_offset: non_neg_integer() | nil,
          fork_sub_offset: non_neg_integer(),
          content_type: binary() | nil
        }
end

defmodule Slap.Streams.Store.PublishedCopy do
  @moduledoc false
  alias Slap.Streams.Store.Identity

  @enforce_keys [:source, :target, :source_placement_key, :requested_offset, :offset]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          source: Identity.t(),
          target: Identity.t(),
          source_placement_key: binary(),
          requested_offset: non_neg_integer() | nil,
          offset: non_neg_integer()
        }
end

defmodule Slap.Streams.Store.PublishedReference do
  @moduledoc false
  alias Slap.Streams.Store.Capture

  @enforce_keys [:capture, :source_placement_key, :requested_offset, :sub_offset]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          capture: Capture.t(),
          source_placement_key: binary(),
          requested_offset: non_neg_integer() | nil,
          sub_offset: non_neg_integer()
        }
end
