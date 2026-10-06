defmodule Slap.Yjs.DocServer.Appends do
  @moduledoc false
  # A document server's appends: the frames buffered, the one append
  # running, and the flush timer. What is buffered while an append runs
  # goes in the next one, once that is acknowledged; a failed append's body
  # goes back in front of the buffer, for a retry after a backoff.

  @retry_min 50
  @retry_max 2_000

  # The message the flush timer sends the server.
  @flush {Slap.Yjs.DocServer, :flush}

  @type t :: %__MODULE__{
          flush_after: non_neg_integer(),
          flush_bytes: non_neg_integer(),
          buffer: iodata(),
          bytes: non_neg_integer(),
          count: non_neg_integer(),
          timer: reference() | nil,
          backoff: pos_integer(),
          running: %{body: binary(), count: non_neg_integer()} | nil,
          acked: non_neg_integer()
        }

  @enforce_keys [:flush_after, :flush_bytes]
  defstruct [
    :flush_after,
    :flush_bytes,
    buffer: [],
    bytes: 0,
    # Buffered updates (the buffer holds their frames).
    count: 0,
    timer: nil,
    backoff: @retry_min,
    running: nil,
    # Appends acknowledged.
    acked: 0
  ]

  @spec new(keyword() | map()) :: t()
  def new(opts),
    do: %__MODULE__{flush_after: opts[:flush_after], flush_bytes: opts[:flush_bytes]}

  @spec add(t(), binary()) :: {:full | :buffered, t()}
  def add(a, frame) do
    a = %{a | buffer: [a.buffer, frame], bytes: a.bytes + byte_size(frame), count: a.count + 1}
    {if(a.bytes >= a.flush_bytes, do: :full, else: :buffered), a}
  end

  @spec schedule(t()) :: t()
  def schedule(%{timer: nil} = a),
    do: %{a | timer: Process.send_after(self(), @flush, a.flush_after)}

  def schedule(a), do: a

  @spec timer_fired(t()) :: t()
  def timer_fired(a), do: %{a | timer: nil}

  @spec take(t()) :: {:append, binary(), pos_integer(), t()} | :none
  def take(%{bytes: 0}), do: :none
  def take(%{running: %{}}), do: :none

  def take(a) do
    if a.timer, do: Process.cancel_timer(a.timer)
    body = IO.iodata_to_binary(a.buffer)
    running = %{body: body, count: a.count}
    {:append, body, a.count, %{a | buffer: [], bytes: 0, count: 0, timer: nil, running: running}}
  end

  @spec acked(t()) :: t()
  def acked(a), do: %{a | running: nil, backoff: @retry_min, acked: a.acked + 1}

  @spec retry(t()) :: t()
  def retry(%{running: r} = a) do
    if a.timer, do: Process.cancel_timer(a.timer)

    %{
      a
      | running: nil,
        buffer: [r.body, a.buffer],
        bytes: a.bytes + byte_size(r.body),
        count: a.count + r.count,
        timer: Process.send_after(self(), @flush, a.backoff),
        backoff: min(a.backoff * 2, @retry_max)
    }
  end

  @spec idle?(t()) :: boolean()
  def idle?(a), do: a.bytes == 0 and a.running == nil
end
