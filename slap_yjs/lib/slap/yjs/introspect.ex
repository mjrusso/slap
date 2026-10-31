defmodule Slap.Yjs.Introspect do
  @moduledoc false

  alias Slap.Cluster.Inspection
  alias Slap.Streams.Introspect, as: Streams
  alias Slap.Streams.Introspect.Messages
  alias Slap.Yjs.Frame
  alias Slap.Yjs.Reconstruction

  def limits, do: Reconstruction.limits()

  # Reconstruction reads nothing: it can run on any node, and its input can
  # come from several shards.
  def reconstruct(version, budget, input),
    do:
      Inspection.exclusive(version, budget, fn deadline ->
        Reconstruction.run(input, deadline)
      end)

  def messages(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      with {:ok, page} <- Streams.messages(ctx, deadline, opts),
           do: {:ok, Messages.annotate(page, &decode/1)}
    end)
  end

  def decode(binary) do
    with {:ok, frames} <- Frame.parse(binary),
         do:
           {:ok,
            %{
              frame_count: length(frames),
              frame_sizes: frames |> Enum.take(64) |> Enum.map(&byte_size/1),
              sizes_truncated: Enum.count_until(frames, 65) > 64,
              bytes: byte_size(binary)
            }}
  end
end
