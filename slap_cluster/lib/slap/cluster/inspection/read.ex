defmodule Slap.Cluster.Inspection.Read do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.SlateDB.Iterator

  @max_key 4096
  @max_value 512 * 1024
  @target 1024 * 1024
  @allowance 4096

  def max_key_bytes, do: @max_key

  def deadline(budget), do: System.monotonic_time(:millisecond) + budget

  def storage_error(%Slap.SlateDB.Error{} = error),
    do: {:error, {:storage, error.kind, error.message}}

  def expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  def check_deadline(deadline), do: if(expired?(deadline), do: {:error, :deadline}, else: :ok)

  def limits(opts) do
    with {:ok, key} <- bounded(opts, :max_key_bytes, @max_key),
         {:ok, value} <- bounded(opts, :max_value_bytes, @max_value),
         {:ok, target} <- bounded(opts, :target_bytes, @target),
         {:ok, rows} <- bounded(opts, :limit, 200) do
      {:ok, %{key: key, value: value, target: target, rows: rows}}
    end
  end

  defp bounded(opts, key, maximum) do
    case Keyword.get(opts, key, maximum) do
      n when is_integer(n) and n > 0 and n <= maximum -> {:ok, n}
      _ -> {:error, {:invalid, "#{key} must be in 1..#{maximum}"}}
    end
  end

  def page(ctx, deadline, opts, decode),
    do: read_page(ctx, deadline, opts, &decoded(&1, decode))

  @doc """
  Reads rows whose keys `select` accepts. `select.(key)` returns decoded
  fields or `:skip`. Skipped rows count against the deadline but not the
  page's row or byte limits. Truncated keys bypass `select` and end the
  page with a diagnostic.
  """
  def filtered(ctx, deadline, opts, select) do
    read_page(ctx, deadline, opts, fn
      %{oversized_key: true} = row -> oversized(row)
      row -> with decoded when decoded != :skip <- select.(row.key), do: listed(row, decoded)
    end)
  end

  # `build.(row)` returns a page row or `:skip`.
  defp read_page(ctx, deadline, opts, build) do
    with {:ok, limits} <- limits(opts),
         do: page(ctx, deadline, opts, build, limits)
  end

  defp page(ctx, deadline, opts, build, limits) do
    bounds = opts |> Keyword.take([:prefix, :gte, :gt, :lt, :lte]) |> absolute_bounds()

    bounds =
      cursor(bounds, Keyword.get(opts, :cursor), Keyword.get(opts, :cursor_exclusive, false))

    {bounds, stop} = lookahead(bounds, Keyword.get(opts, :lookahead_lt))
    start = {Keyword.get(bounds, :gte, Keyword.get(bounds, :gt, <<>>)), is_binary(bounds[:gt])}

    scan = scan(ctx, deadline, limits, build: build, stop: stop)

    cond do
      expired?(deadline) ->
        finish(scan, start, :deadline)

      empty_bounds?(bounds) ->
        finish(scan, nil, :end)

      true ->
        with {:ok, iter} <- SlateDB.preview_iterator(ctx.db, bounds ++ read_opts(limits)),
             do: collect(iter, scan, start)
    end
  end

  @doc """
  Reads the last rows within the key bounds, subject to page limits.
  Returns `{:ok, %{rows: rows, stopped: :end | :limit}}`, with rows in
  ascending key order.

  A forward read from the first returned key reaches the upper bound within
  the same page limits, provided the keys are complete and the bounds and
  decoder are unchanged. Truncated keys can stop that page early.
  """
  def last(ctx, deadline, opts, decode) do
    with {:ok, limits} <- limits(opts) do
      bounds = opts |> Keyword.take([:prefix, :gte, :gt, :lt, :lte]) |> absolute_bounds()
      scan = scan(ctx, deadline, limits, build: &decoded(&1, decode))

      if empty_bounds?(bounds),
        do: {:ok, %{rows: [], stopped: :end}},
        else: last(ctx, bounds, scan)
    end
  end

  defp last(ctx, bounds, scan) do
    with {:ok, iter} <-
           SlateDB.preview_iterator(ctx.db, bounds ++ [order: :desc] ++ read_opts(scan.limits)),
         do: descend(iter, scan)
  end

  defp descend(iter, scan) do
    cond do
      expired?(scan.deadline) ->
        {:error, :deadline}

      full?(scan) ->
        {:ok, %{rows: within_target(scan), stopped: :limit}}

      true ->
        case Iterator.next_batch(iter, 1) do
          {:ok, []} ->
            {:ok, %{rows: scan.rows, stopped: :end}}

          {:ok, [{key, value, seq, key_size, value_size}]} ->
            row = key |> row(value, key_size, value_size) |> Map.put(:seq, seq)
            descend(iter, push(scan, scan.build.(row)))

          {:error, error} ->
            storage_error(error)
        end
    end
  end

  # A forward page stops at the byte target. Drop leading rows until only
  # the last row can reach or exceed it.
  defp within_target(%{rows: rows, bytes: bytes, limits: %{target: target}}) do
    last = rows |> List.last() |> :erlang.external_size()
    drop_until_under(rows, bytes - last, target)
  end

  defp drop_until_under([_last] = rows, _before_last, _target), do: rows

  defp drop_until_under([first | rest] = rows, before_last, target) do
    if before_last < target,
      do: rows,
      else: drop_until_under(rest, before_last - :erlang.external_size(first), target)
  end

  @doc """
  Reads one row per key group from `:cursor`, without reading values.
  `group.(key, key_size)` receives the key preview and its full size. It
  returns `{:list, scope, decoded}` to list a row or `{:skip, scope}` to skip
  it. The scope is `:key` for the current key or `{:prefix, prefix}` for all
  keys starting with `prefix`. Listed rows use the current key or the prefix
  as their key, and `decoded` as their decoded fields.
  For a truncated key, `:key` advances past all keys sharing its preview.

  An `:unknown` result lists the key with an `:unknown_key` diagnostic.
  If an unknown key exceeds `:max_key_bytes`, it is listed with an
  oversized-key diagnostic, and keys starting with its preview are skipped.
  """
  def groups(ctx, deadline, opts, group) do
    with {:ok, limits} <- limits(opts) do
      start = Keyword.get(opts, :cursor, <<>>)
      scan = scan(ctx, deadline, limits, group: group)
      if expired?(deadline), do: finish(scan, start, :deadline), else: open_groups(scan, start)
    end
  end

  defp open_groups(scan, start) do
    read_opts = [gte: start] ++ read_opts(%{scan.limits | value: 0})

    with {:ok, iter} <- SlateDB.preview_iterator(scan.ctx.db, read_opts),
         do: next_group(iter, scan, start)
  end

  defp next_group(iter, scan, cursor) do
    if expired?(scan.deadline) do
      finish(scan, cursor, :deadline)
    else
      case Iterator.next_batch(iter, 1) do
        {:ok, []} -> finish(scan, nil, :end)
        {:ok, [{key, _value, _seq, key_size, _}]} -> group(iter, scan, key, key_size)
        {:error, error} -> storage_error(error)
      end
    end
  end

  defp group(iter, scan, key, key_size) do
    {row, next} =
      case scan.group.(key, key_size) do
        {:list, scope, decoded} ->
          {key, size, next} = group_scope(key, key_size, scope)
          {key |> row(<<>>, size, 0) |> listed(decoded), next}

        {:skip, scope} ->
          {_key, _size, next} = group_scope(key, key_size, scope)
          {nil, next}

        :unknown when key_size > byte_size(key) ->
          {oversized(row(key, <<>>, key_size, 0)), successor(key)}

        :unknown ->
          {Map.put(row(key, <<>>, key_size, 0), :diagnostic, :unknown_key), key <> <<0>>}
      end

    scan = if row, do: push(scan, row), else: scan

    advance(iter, scan, next)
  end

  defp group_scope(key, size, :key) when size > byte_size(key),
    do: {key, size, successor(key)}

  defp group_scope(key, size, :key), do: {key, size, key <> <<0>>}

  defp group_scope(_key, _size, {:prefix, prefix}),
    do: {prefix, byte_size(prefix), successor(prefix)}

  defp advance(_iter, scan, nil), do: finish(scan, nil, :end)

  defp advance(iter, scan, next) do
    cond do
      expired?(scan.deadline) ->
        finish(scan, next, :deadline)

      full?(scan) ->
        finish(scan, next, :limit)

      true ->
        case Iterator.seek(iter, next) do
          :ok -> next_group(iter, scan, next)
          {:error, error} -> storage_error(error)
        end
    end
  end

  # With `:lookahead_lt`, the scan continues past the page's upper bound to
  # that key, so a page that stops at `:end` or `:limit` reports the first key
  # after its rows as `next_key` (nil if none is before `:lookahead_lt`).
  defp lookahead(bounds, nil), do: {bounds, nil}

  defp lookahead(bounds, lt) when is_binary(lt),
    do:
      {bounds |> Keyword.drop([:lt, :lte]) |> Keyword.put(:lt, lt),
       Keyword.take(bounds, [:lt, :lte])}

  # A key cut to `max_key_bytes` is past the bound only if its preview shows it.
  defp past_stop?([lt: bound], key, size), do: order(key, size, bound) in [:gt, :eq]
  defp past_stop?([lte: bound], key, size), do: order(key, size, bound) == :gt
  defp past_stop?(_stop, _key, _size), do: false

  defp order(key, size, bound) when size == byte_size(key) do
    cond do
      key > bound -> :gt
      key < bound -> :lt
      true -> :eq
    end
  end

  defp order(prefix, _size, bound) do
    head = binary_part(bound, 0, min(byte_size(prefix), byte_size(bound)))

    cond do
      prefix > head -> :gt
      prefix < head -> :lt
      byte_size(bound) <= byte_size(prefix) -> :gt
      true -> :unknown
    end
  end

  defp full?(scan), do: length(scan.rows) >= scan.limits.rows or scan.bytes >= scan.limits.target

  defp push(scan, row),
    do: %{scan | rows: [row | scan.rows], bytes: scan.bytes + :erlang.external_size(row)}

  defp oversized(row), do: Map.put(row, :diagnostic, :oversized_keys_with_prefix_skipped)

  defp scan(ctx, deadline, limits, fields),
    do:
      Map.merge(
        %{ctx: ctx, deadline: deadline, limits: limits, stop: nil, rows: [], bytes: 0},
        Map.new(fields)
      )

  defp empty_bounds?(bounds) do
    lower = bounds[:gte] || bounds[:gt]
    upper = bounds[:lte] || bounds[:lt]

    is_binary(lower) and is_binary(upper) and
      (lower > upper or
         (lower == upper and not (is_binary(bounds[:gte]) and is_binary(bounds[:lte]))))
  end

  defp cursor(bounds, nil, _exclusive), do: bounds

  defp cursor(bounds, key, exclusive) when is_binary(key) and is_boolean(exclusive) do
    lower = bounds[:gte] || bounds[:gt]

    if is_binary(lower) and (lower > key or (lower == key and is_binary(bounds[:gt]))) do
      bounds
    else
      kind = if exclusive, do: :gt, else: :gte
      bounds |> Keyword.drop([:gte, :gt]) |> Keyword.put(kind, key)
    end
  end

  defp absolute_bounds(bounds) do
    case Keyword.pop(bounds, :prefix) do
      {nil, bounds} ->
        bounds

      {prefix, bounds} ->
        # SlateDB's prefix bounds are suffixes; inspection cursors are full keys.
        bounds = Enum.map(bounds, fn {kind, suffix} -> {kind, prefix <> suffix} end)

        bounds =
          if bounds[:gte] || bounds[:gt], do: bounds, else: Keyword.put(bounds, :gte, prefix)

        case {bounds[:lt] || bounds[:lte], successor(prefix)} do
          {nil, upper} when is_binary(upper) -> Keyword.put(bounds, :lt, upper)
          _ -> bounds
        end
    end
  end

  defp read_opts(limits) do
    # SlateDB requires a positive prefetch target when scanning SST files.
    [
      durability: :remote,
      cache_blocks: false,
      read_ahead_bytes: 1,
      max_fetch_tasks: 1,
      max_key_bytes: limits.key,
      max_value_bytes: limits.value
    ]
  end

  defp collect(iter, scan, cursor) do
    cond do
      expired?(scan.deadline) ->
        finish(scan, cursor, :deadline)

      full?(scan) ->
        full(iter, scan, cursor)

      true ->
        fetch(iter, scan)
    end
  end

  defp full(_iter, %{stop: nil} = scan, cursor), do: finish(scan, cursor, :limit)

  defp full(iter, scan, cursor) do
    case Iterator.next_batch(iter, 1) do
      {:ok, []} -> finish(scan, cursor, :limit)
      {:ok, [{key, _value, _seq, _key_size, _value_size}]} -> finish(scan, cursor, :limit, key)
      {:error, error} -> storage_error(error)
    end
  end

  defp fetch(iter, scan) do
    case Iterator.next_batch(iter, 1) do
      {:ok, []} ->
        finish(scan, nil, :end)

      {:ok, [{key, value, seq, key_size, value_size}]} ->
        if past_stop?(scan.stop, key, key_size),
          do: finish(scan, nil, :end, key),
          else: add(iter, scan, key |> row(value, key_size, value_size) |> Map.put(:seq, seq))

      {:error, error} ->
        storage_error(error)
    end
  end

  defp add(iter, scan, row) do
    case scan.build.(row) do
      :skip -> collect(iter, scan, next_cursor(row.key, scan.limits.key))
      row -> keep(iter, scan, row)
    end
  end

  defp keep(iter, scan, row) do
    if row.oversized_key,
      do: finish(%{scan | rows: [row | scan.rows]}, successor(row.key), :oversized_keys_skipped),
      else: collect(iter, push(scan, row), next_cursor(row.key, scan.limits.key))
  end

  defp row(key, value, key_size, value_size) do
    %{
      key: key,
      value: value,
      key_size: key_size,
      value_size: value_size,
      oversized_key: key_size > byte_size(key),
      truncated: value_size > byte_size(value)
    }
  end

  defp value_row(key, value, size, offset) do
    row = row(key, value, byte_size(key), size)
    Map.merge(row, %{offset: offset, truncated: row.truncated or offset != 0})
  end

  defp listed(row, decoded) do
    if :erlang.external_size(decoded) <= @allowance,
      do: Map.put(row, :decoded, decoded),
      else: Map.put(row, :diagnostic, :decoded_fields_too_large)
  end

  defp decoded(%{oversized_key: true} = row, _decode), do: oversized(row)

  defp decoded(%{truncated: true} = row, _decode), do: Map.put(row, :diagnostic, :truncated_value)

  # Decoders may raise on malformed stored rows; this is the one place that
  # turns that into a diagnostic.
  defp decoded(row, decode) do
    listed(row, decode.(row.key, row.value))
  rescue
    _error in [
      ArgumentError,
      KeyError,
      MatchError,
      CaseClauseError,
      FunctionClauseError,
      BadMapError,
      Protocol.UndefinedError
    ] ->
      Map.put(row, :diagnostic, :malformed_record)
  end

  def get(ctx, deadline, key, opts, decode) when byte_size(key) <= @max_key do
    with {:ok, limits} <- limits(opts),
         :ok <- check_deadline(deadline) do
      range = Keyword.take(opts, [:value_range])

      case SlateDB.get_range(
             ctx.db,
             key,
             range ++ [durability: :remote, cache_blocks: false, max_value_bytes: limits.value]
           ) do
        {:ok, nil} ->
          {:ok, stamp(ctx, %{row: nil})}

        {:ok, %{value: value, value_size: size, offset: offset}} ->
          {:ok, stamp(ctx, %{row: key |> value_row(value, size, offset) |> decoded(decode)})}

        {:error, error} ->
          storage_error(error)
      end
    end
  end

  def get(_ctx, _deadline, _key, _opts, _decode), do: {:error, :oversized_key}

  @doc """
  `key`'s durable value as `%{value: bytes, value_size: size}`, with at most
  the first 512 KiB of it, or nil. `row/5` bounds what is displayed of it.
  """
  def fetch(ctx, deadline, key) do
    with :ok <- check_deadline(deadline) do
      opts = [durability: :remote, cache_blocks: false, max_value_bytes: @max_value]

      case SlateDB.get_range(ctx.db, key, opts) do
        {:ok, fetched} -> {:ok, fetched}
        {:error, error} -> storage_error(error)
      end
    end
  end

  @doc """
  The reply `get/5` gives for `key`, built from a value `fetch/3` read, or
  `{:error, :oversized_value}` when that reply needs bytes past the 512 KiB
  `fetch/3` keeps.
  """
  def row(ctx, key, fetched, opts, decode) when byte_size(key) <= @max_key do
    with {:ok, limits} <- limits(opts),
         :ok <- covered(fetched, opts, limits.value),
         do: {:ok, stamp(ctx, %{row: fetched && slice(key, fetched, opts, limits.value, decode)})}
  end

  def row(_ctx, _key, _fetched, _opts, _decode), do: {:error, :oversized_key}

  defp covered(nil, _opts, _max), do: :ok

  defp covered(%{value: value, value_size: size}, opts, max) do
    {offset, length} = Keyword.get(opts, :value_range, {0, size})
    stop = min(offset + min(length, max), size)

    if stop <= offset or stop <= byte_size(value),
      do: :ok,
      else: {:error, :oversized_value}
  end

  # The bytes `SlateDB.get_range/3` would copy, so the reply does not keep
  # the fetched value alive.
  defp slice(key, %{value: value, value_size: size}, opts, max, decode) do
    available = byte_size(value)
    {offset, length} = Keyword.get(opts, :value_range, {0, size})
    start = min(offset, available)
    part = binary_part(value, start, length |> min(max) |> min(available - start))
    key |> value_row(:binary.copy(part), size, offset) |> decoded(decode)
  end

  defp stamp(ctx, fields),
    do:
      Map.merge(fields, %{owner: node(), shard: ctx.n, durable_seq: Slap.Cluster.durable_seq(ctx)})

  defp next_cursor(key, maximum) when byte_size(key) == maximum, do: {key, true}
  defp next_cursor(key, _maximum), do: key <> <<0>>

  defp finish(scan, cursor, stopped, next_key \\ nil)

  defp finish(scan, {cursor, exclusive}, stopped, next_key) do
    page = %{
      rows: Enum.reverse(scan.rows),
      cursor: cursor,
      cursor_exclusive: exclusive,
      stopped: stopped
    }

    page = if scan.stop, do: Map.put(page, :next_key, next_key), else: page
    {:ok, envelope(scan.ctx, page, scan.limits)}
  end

  defp finish(scan, cursor, stopped, next_key),
    do: finish(scan, {cursor, false}, stopped, next_key)

  defp envelope(ctx, page, limits),
    do:
      stamp(
        ctx,
        Map.put(
          page,
          :hard_max_bytes,
          limits.target + limits.key * 2 + limits.value + @allowance + 8192
        )
      )

  defp successor(<<>>), do: nil

  defp successor(prefix) do
    size = byte_size(prefix) - 1

    case prefix do
      <<head::binary-size(^size), 255>> -> successor(head)
      <<head::binary-size(^size), last>> -> <<head::binary, last + 1>>
    end
  end
end
