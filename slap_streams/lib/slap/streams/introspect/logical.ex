defmodule Slap.Streams.Introspect.Logical do
  @moduledoc false

  alias Slap.Streams.Introspect.Cursor
  alias Slap.Streams.Store.Meta

  def metadata(%Meta{} = meta),
    do: %{pending: Meta.pending?(meta), reference: reference(Meta.reference(meta))}

  defp reference(nil), do: nil

  defp reference(capture) do
    capture
    |> Map.take([:start, :offset, :depth, :content_type, :closed, :ttl_s, :expires_at_ms])
    |> Map.merge(%{
      source: Map.from_struct(capture.source),
      target: Map.from_struct(capture.target)
    })
  end

  @doc "Plans a logical read of the stream on `shard` from its stored state (`Introspect.state/3`)."
  def describe(state, shard, opts) do
    with {:ok, %{meta: stored, tail: tail, trim: trim}} <- known(state),
         meta = Map.merge(Map.from_struct(stored), metadata(stored)),
         :ok <- identity(meta, opts[:expected_sid]),
         :ok <- visible(meta, opts),
         trim = if(opts[:retained], do: 0, else: trim),
         :ok <- available(tail),
         {:ok, position} <- Cursor.position(opts[:cursor]),
         from = Enum.max([Keyword.get(opts, :from, 0), trim, position]),
         until = min(Keyword.get(opts, :until, tail), tail),
         {:ok, plan} <-
           read_plan(meta, shard, Keyword.merge(opts, from: from, until: until)) do
      {:ok,
       Map.merge(plan, %{
         sid: meta.sid,
         from: from,
         until: until,
         reference: meta.reference,
         soft_deleted: meta.soft_deleted,
         creation: creation(meta)
       })}
    end
  end

  defp read_plan(%{reference: nil} = meta, shard, opts), do: own_plan(meta, shard, opts)

  defp read_plan(%{reference: cap} = meta, shard, opts) do
    with {:ok, remaining} <- reference_budget(cap, opts[:reference_budget]) do
      from = max(opts[:from], cap.start)
      until = min(opts[:until], cap.offset)

      if from < until do
        source_plan(cap, opts, from, until, remaining)
      else
        own_plan(meta, shard, Keyword.put(opts, :from, max(opts[:from], cap.offset)))
      end
    end
  end

  defp reference_budget(%{depth: depth}, nil) when is_integer(depth) and depth >= 0,
    do: {:ok, depth}

  defp reference_budget(_cap, remaining) when is_integer(remaining) and remaining >= 0,
    do: {:ok, remaining}

  defp reference_budget(_cap, _remaining), do: {:error, :lineage_depth_unavailable}

  defp source_plan(_cap, _opts, _from, _until, 0), do: {:error, :lineage_depth_limit}

  defp source_plan(cap, opts, from, until, remaining) do
    source =
      opts
      |> Keyword.delete(:placement_key)
      |> Keyword.merge(
        path: cap.source.path,
        expected_sid: cap.source.sid,
        from: from,
        until: until,
        retained: true,
        reference_budget: remaining - 1
      )

    continuation = if cap.offset < opts[:until], do: Cursor.at(cap.offset)

    {:ok,
     %{
       read_plan: %{kind: :source, shard: cap.source.shard, opts: source},
       continuation: continuation
     }}
  end

  defp own_plan(meta, shard, opts) do
    {:ok,
     %{
       read_plan: %{
         kind: :messages,
         shard: shard,
         opts: Keyword.merge(opts, sid: meta.sid, logical_cursor: true)
       },
       continuation: nil
     }}
  end

  defp creation(%{pending: true}), do: :pending
  defp creation(%{reference: reference}) when not is_nil(reference), do: :reference
  defp creation(%{creation: nil}), do: :ordinary
  defp creation(_meta), do: :copy

  defp identity(_meta, nil), do: :ok
  defp identity(%{sid: sid}, sid), do: :ok
  defp identity(_meta, _sid), do: {:error, :stream_identity_changed}

  defp visible(%{soft_deleted: true}, opts) do
    if opts[:retained], do: :ok, else: {:error, :soft_deleted}
  end

  defp visible(%{pending: true}, _opts), do: {:error, :creation_pending}
  defp visible(_meta, _opts), do: :ok

  defp known({:ok, nil}), do: {:error, :not_found_custom_placement_key_possible}
  defp known(state), do: state

  defp available(tail) when is_integer(tail), do: :ok
  defp available(nil), do: {:error, :tail_unavailable}
end
