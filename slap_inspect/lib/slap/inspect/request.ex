defmodule Slap.Inspect.Request do
  @moduledoc false

  alias Slap.Inspect.Operation

  @enforce_keys [:view, :shard, :budget, :opts]
  # `observer` receives `{:inspection_owner, owner, shard}` before each owner read.
  # With `count_others`, an empty whole-shard listing searches other shards
  # and switches to the first with rows.
  defstruct [:operation, :observer, count_others: false] ++ @enforce_keys

  @range_bytes 4096

  def new(%{view: view, shard: shard, budget: budget, opts: opts} = request) do
    with {:ok, operation, definition} <- Operation.normalize(view, opts),
         budget = if(is_nil(budget), do: Operation.budget(operation), else: budget),
         :ok <- validate_numbers(opts, definition.view),
         opts = Keyword.take(opts, Operation.page(definition) ++ definition.options),
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
         observer: Map.get(request, :observer),
         count_others: Map.get(request, :count_others, false)
       }}
    end
  end

  @doc "`request` for the page after `page`, or nil when `page` is the last."
  def next(request, %{cursor: cursor} = page) when is_binary(cursor),
    do: %{
      request
      | opts:
          request.opts
          |> Keyword.delete(:before)
          |> Keyword.merge(cursor: cursor, cursor_exclusive: page[:cursor_exclusive] || false)
    }

  def next(_request, _page), do: nil

  @doc """
  Builds a shard-picker request that counts rows on each shard when
  `request` lists a whole shard.
  """
  def shards(%__MODULE__{operation: operation} = request) do
    if Operation.spans_shard?(operation),
      do: %{view: :shards, budget: 5000, listing: request},
      else: %{view: :shards, budget: 5000}
  end

  @doc """
  A request for `view` built from `request`: the same shard, budget and
  observer, with the operation and options checked again for `view`.
  """
  def derive(request, view, opts), do: new(%{request | view: view, opts: opts})

  @doc """
  Builds a request for a view opened from the menu or home. Preserves page
  limits and a custom budget. Keeps the shard when switching between shard
  listings, and clears the previous view's base and path.
  """
  def draft(request, view),
    do: %{
      view: view,
      shard: listed_shard(request, view),
      budget: chosen_budget(request),
      opts: Keyword.take(request.opts, [:limit | Operation.read_limits()])
    }

  defp listed_shard(%__MODULE__{operation: operation, shard: shard}, view) do
    case Operation.normalize(view, []) do
      {:ok, next, _definition} ->
        if Operation.spans_shard?(operation) and Operation.spans_shard?(next), do: shard

      {:error, _reason} ->
        nil
    end
  end

  @doc """
  Returns a custom budget, or nil to use the next view's default.
  """
  def chosen_budget(%__MODULE__{operation: operation, budget: budget}),
    do: if(budget == Operation.budget(operation), do: nil, else: budget)

  def refresh(interval) do
    numeric_ranges([refresh: interval], refresh: {0, 4_294_967_295})
  end

  @doc "The bytes a byte or text view shows at a time."
  def range_bytes, do: @range_bytes

  def bytes(request, key, offset) when is_integer(offset),
    do: bytes(request, key, {offset, @range_bytes})

  def bytes(request, key, {offset, length}) do
    length = min(length, request.opts[:max_value_bytes] || length)
    limits = Keyword.take(request.opts, Operation.read_limits())

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
      do: {:error, {:required_options, [definition.required]}},
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

    numbers = if opts[:before] == :end, do: Keyword.delete(opts, :before), else: opts
    ranges = [{:before, {0, 0xFFFFFFFFFFFFFFFF}} | ranges]

    with :ok <- numeric_ranges(numbers, ranges),
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
