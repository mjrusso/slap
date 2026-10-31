defmodule Slap.Inspect.Links do
  @moduledoc false

  # The views a row links to, as `{:ok, view, opts, shard}` (a nil shard keeps
  # the current one) or `:none`.

  def follow(%{decoded: {:ok, %{kind: :registration, value: %{target: target}}}}, _request),
    do: {:ok, :logical, [path: target.path, expected_sid: target.sid], target.shard}

  def follow(%{decoded: {:ok, %{value: %{body: [:object, key]}}}}, request) do
    opts = Keyword.take(request.opts, [:instance])
    {:ok, :object, Keyword.put(opts, :object_key, key), nil}
  end

  def follow(%{decoded: {:ok, %{path: path}}}, _request), do: {:ok, :logical, [path: path], nil}

  def follow(%{decoded: {:ok, %{value: %{source: source}}}}, _request),
    do: {:ok, :logical, [path: source.path, expected_sid: source.sid], source.shard}

  def follow(_row, _request), do: :none

  def retention(%{decoded: {:ok, %{path: path, value: %{sid: sid}}}}),
    do: {:ok, :retention, [path: path, sid: sid], nil}

  def retention(_row), do: :none
end
