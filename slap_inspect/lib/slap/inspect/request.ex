defmodule Slap.Inspect.Request do
  @moduledoc false

  alias Slap.Inspect.Operation

  @enforce_keys [:view, :shard, :budget, :opts]
  # `observer` receives `{:inspection_owner, owner, shard}` before each owner read.
  defstruct [:operation, :observer | @enforce_keys]

  @read_limits [:max_key_bytes, :max_value_bytes, :target_bytes]
  @range_bytes 4096
  @page [:limit, :cursor, :cursor_exclusive] ++ @read_limits

  def new(%{view: view, shard: shard, budget: budget, opts: opts} = request) do
    with {:ok, operation, definition} <- Operation.normalize(view, opts),
         budget = if(is_nil(budget), do: Operation.budget(operation), else: budget),
         :ok <- validate_numbers(opts, definition.view),
         opts = Keyword.take(opts, @page ++ definition.options),
         :ok <-
           numeric_ranges([shard: shard, budget: budget],
             shard: {0, nil},
             budget: {1, 30_000}
           ),
         :ok <- required_read_options(definition, opts) do
      {:ok,
       %__MODULE__{
         view: definition.view,
         operation: operation,
         shard: shard,
         budget: budget,
         opts: opts,
         observer: Map.get(request, :observer)
       }}
    end
  end

  def read_limits, do: @read_limits
  def page_options, do: @page

  @doc "`request` for the page after `page`, or nil when `page` is the last."
  def next(request, %{cursor: cursor} = page) when is_binary(cursor),
    do: %{
      request
      | opts:
          Keyword.merge(request.opts,
            cursor: cursor,
            cursor_exclusive: page[:cursor_exclusive] || false
          )
    }

  def next(_request, _page), do: nil

  @doc "A request for the cluster's shards, for the shard picker."
  def shards, do: %{view: :shards, budget: 5000}

  @doc """
  A request for `view` built from `request`: the same shard, budget and
  observer, with the operation and options checked again for `view`.
  """
  def derive(request, view, opts), do: new(%{request | view: view, opts: opts})

  def draft(request, view) do
    opts = Keyword.drop(request.opts, [:cursor, :cursor_exclusive])

    %{
      view: view,
      shard: request.shard,
      budget: request.budget,
      opts: Keyword.take(opts, @page ++ Operation.options(view))
    }
  end

  def refresh(interval) do
    numeric_ranges([refresh: interval], refresh: {0, 4_294_967_295})
  end

  @doc "The bytes a byte or text view shows at a time."
  def range_bytes, do: @range_bytes

  def bytes(request, key, offset) when is_integer(offset),
    do: bytes(request, key, {offset, @range_bytes})

  def bytes(request, key, {offset, length}) do
    length = min(length, request.opts[:max_value_bytes] || length)
    limits = Keyword.take(request.opts, @read_limits)

    {view, opts} =
      case request.view do
        :object ->
          {:object, Keyword.put(Keyword.take(request.opts, [:instance]), :object_key, key)}

        _ ->
          {:get, [key: key]}
      end

    derive(request, view, Keyword.put(limits ++ opts, :value_range, {offset, length}))
  end

  defp required_read_options(definition, opts) do
    if Enum.any?(definition.required, &is_nil(opts[&1])),
      do: {:error, {:required_options, Operation.required(definition.view)}},
      else: :ok
  end

  defp validate_numbers(opts, view) do
    # Only types are checked here; the server enforces its own maximums.
    ranges = [
      sid: {0, 0xFFFFFFFFFFFFFFFF},
      expected_sid: {0, 0xFFFFFFFFFFFFFFFF},
      from: {0, 0xFFFFFFFFFFFFFFFF},
      until: {0, 0xFFFFFFFFFFFFFFFF},
      at_offset: {0, 0xFFFFFFFFFFFFFFFF},
      limit: {1, nil},
      max_key_bytes: {1, nil},
      max_value_bytes: {1, nil},
      target_bytes: {1, nil},
      max_input_bytes: {1, nil},
      max_entries: {1, nil},
      max_output_bytes: {1, nil}
    ]

    with :ok <- numeric_ranges(opts, ranges),
         :ok <- value_range(opts, view),
         do: ordered_range(opts)
  end

  defp numeric_ranges(opts, ranges) do
    Enum.reduce_while(ranges, :ok, fn {key, {minimum, maximum}}, :ok ->
      case opts[key] do
        nil ->
          {:cont, :ok}

        value
        when is_integer(value) and value >= minimum and (is_nil(maximum) or value <= maximum) ->
          {:cont, :ok}

        _ ->
          {:halt, {:error, {:invalid_option, key, minimum, maximum}}}
      end
    end)
  end

  defp ordered_range(opts) do
    if is_integer(opts[:from]) and is_integer(opts[:until]) and opts[:from] > opts[:until],
      do: {:error, {:invalid, "--until must be greater than or equal to --from"}},
      else: :ok
  end

  defp value_range(opts, view) do
    case opts[:value_range] do
      nil ->
        :ok

      {offset, length} ->
        minimum = if view == :object, do: 1, else: 0

        numeric_ranges([offset: offset, length: length],
          offset: {0, 0xFFFFFFFFFFFFFFFF},
          length: {minimum, opts[:max_value_bytes]}
        )
    end
  end
end
