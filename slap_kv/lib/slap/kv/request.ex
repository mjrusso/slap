defmodule Slap.KV.Request do
  @moduledoc false

  alias Slap.KV.Keys

  @enforce_keys [:partition, :conditions, :mutations, :bytes, :deadline, :reply_kind]
  defstruct @enforce_keys

  @type condition :: :absent | {:version, non_neg_integer()}
  @type t :: %__MODULE__{
          partition: binary(),
          conditions: [{binary(), binary(), condition()}],
          mutations: [Slap.SlateDB.write_op()],
          bytes: non_neg_integer(),
          deadline: integer() | nil,
          reply_kind: :put | :delete | :batch | :get
        }

  def single(partition, op, condition, deadline),
    do: add(new(partition, deadline, elem(op, 0)), op, condition)

  def get(partition), do: new(partition, nil, :get)

  def batch(_partition, [], _deadline, _limits), do: bad_request(:empty_batch)

  def batch(partition, [_ | _] = ops, deadline, limits),
    do: normalize(ops, new(partition, deadline, :batch), limits, %{})

  def batch(_partition, _ops, _deadline, _limits), do: bad_request(:invalid_batch)

  defp new(partition, deadline, kind) do
    %__MODULE__{
      partition: partition,
      conditions: [],
      mutations: [],
      bytes: 0,
      deadline: deadline,
      reply_kind: kind
    }
  end

  defp normalize([], plan, _limits, _seen),
    do:
      {:ok,
       %{
         plan
         | conditions: Enum.reverse(plan.conditions),
           mutations: Enum.reverse(plan.mutations)
       }}

  defp normalize(_rest, _plan, %{max_batch_ops: max}, seen) when map_size(seen) >= max,
    do: bad_request(:batch_too_large)

  defp normalize(_rest, %{bytes: bytes}, %{max_batch_bytes: max}, _seen) when bytes >= max,
    do: bad_request(:batch_too_large)

  defp normalize([op | rest], plan, limits, seen) do
    with {:ok, op, condition} <- operation(op),
         :ok <- Keys.validate(plan.partition, elem(op, 1)),
         false <- Map.has_key?(seen, elem(op, 1)),
         next <- add(plan, op, condition),
         true <- next.bytes <= limits.max_batch_bytes do
      normalize(rest, next, limits, Map.put(seen, elem(op, 1), true))
    else
      true -> bad_request(:duplicate_key)
      false -> bad_request(:batch_too_large)
      {:error, _} = error -> error
    end
  end

  defp normalize(_tail, _plan, _limits, _seen), do: bad_request(:invalid_batch)

  defp add(plan, op, condition) do
    key = elem(op, 1)
    encoded = Keys.encode(plan.partition, key)
    {mutations, bytes} = mutation(op, encoded, plan.mutations)

    conditions =
      if condition == :any,
        do: plan.conditions,
        else: [{encoded, key, condition} | plan.conditions]

    %{
      plan
      | conditions: conditions,
        mutations: mutations,
        bytes: plan.bytes + bytes
    }
  end

  defp mutation({:put, _key, value}, encoded, mutations),
    do: {[{:put, encoded, value} | mutations], byte_size(encoded) + byte_size(value)}

  defp mutation({:delete, _key}, encoded, mutations),
    do: {[{:delete, encoded} | mutations], byte_size(encoded)}

  defp mutation({:check, _key}, encoded, mutations), do: {mutations, byte_size(encoded)}

  defp operation({:put, key, value}) when is_binary(value), do: {:ok, {:put, key, value}, :any}
  defp operation({:put, _key, _value}), do: bad_request(:invalid_value)

  defp operation({:put, key, value, condition}) do
    with {:ok, op, _} <- operation({:put, key, value}),
         {:ok, condition} <- condition(condition, true),
         do: {:ok, op, condition}
  end

  defp operation({:delete, key}), do: {:ok, {:delete, key}, :any}

  defp operation({:delete, key, condition}) do
    with {:ok, condition} <- condition(condition, false),
         do: {:ok, {:delete, key}, condition}
  end

  defp operation({:check, key, condition}) do
    with {:ok, condition} <- condition(condition, true),
         do: {:ok, {:check, key}, condition}
  end

  defp operation(_op), do: bad_request(:invalid_operation)

  defp condition(:absent, true), do: {:ok, :absent}

  defp condition(version, _absent?) when is_integer(version) and version >= 0,
    do: {:ok, {:version, version}}

  defp condition(_version, _absent?), do: bad_request(:invalid_version)

  defp bad_request(reason), do: {:error, {:bad_request, reason}}
end
