defmodule Slap.Yjs.ConvergenceTest do
  # Three nodes, each running Slap.Streams.Cluster (Distributed strategy, one shared
  # local directory) and Slap.Yjs.Docs, with two simulated clients each on one
  # document. The clients insert unique tokens at random while:
  #
  #   * document servers are killed now and then (their clients rejoin);
  #   * a small compaction threshold makes servers on every node compact
  #     and trim, so followers reload snapshots and compactions race;
  #   * the node that owns the document's shard is killed (the shard moves
  #     while servers append to it);
  #   * a surviving node is paused past its net_ticktime (the others take
  #     it for gone, and it comes back).
  #
  # In the end:
  #
  #   * every surviving client's text equals the document as stored;
  #   * no token appears twice, and every token is one a client inserted;
  #   * every token a surviving client inserted is there, and every token
  #     the killed node's clients had checkpointed (stored, per their
  #     server) before it died;
  #   * every server knows the surviving clients' awareness states, and no
  #     others.
  #
  # Synchronous: it makes this node distributed. It polls for convergence,
  # which crosses processes on three nodes it cannot be told about.
  use ExUnit.Case, async: false

  alias Slap.Yjs.Test.{Client, Peers}

  @moduletag :cluster
  @moduletag timeout: 300_000

  setup do
    Peers.distribute!()
    dir = Path.join(System.tmp_dir!(), "yjs-convergence-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    nodes = for _ <- 1..3, do: Peers.start(dir)
    on_exit(fn -> Enum.each(nodes, &Peers.kill_if_up/1) end)
    %{nodes: nodes}
  end

  test "clients on three nodes converge through server kills, compaction, a node kill and a pause",
       %{nodes: nodes} do
    seed = String.to_integer(System.get_env("SLAP_YJS_PROP_SEED", "#{:rand.uniform(1_000_000)}"))
    :rand.seed(:exsss, seed)
    IO.puts("convergence seed: #{seed} (SLAP_YJS_PROP_SEED)")

    doc_id = {"test", "doc-#{System.unique_integer([:positive])}"}

    clients =
      for {node, i} <- Enum.with_index(nodes),
          j <- 1..2,
          do: {node, Client.start(node, doc_id, "c#{i}#{j}")}

    victim = owner(hd(nodes), doc_id)
    assert victim in nodes
    [paused | _] = survivors = nodes -- [victim]

    for round <- 1..200 do
      edit(clients)
      if rem(round, 50) == 0, do: Peers.kill_server(Enum.random(survivors), doc_id)
      receive after: (5 -> :ok)
    end

    # What the victim's clients have stored must survive them.
    victims = for {^victim, client} <- clients, do: client
    checkpointed = Enum.flat_map(victims, &checkpointed/1)
    Peers.kill(victim)
    clients = for {node, client} <- clients, node != victim, do: {node, client}

    for _ <- 1..100 do
      edit(clients)
      receive after: (5 -> :ok)
    end

    # Past net_ticktime: the other node takes the paused node's shards.
    os_pid = Peers.os_pid(paused)
    Peers.pause(os_pid)

    for {node, client} <- clients, node != paused do
      for _ <- 1..20, do: Client.edit(client)
    end

    receive after: (6_000 -> :ok)
    Peers.resume(os_pid)

    for _ <- 1..100 do
      edit(clients)
      receive after: (5 -> :ok)
    end

    survivors_clients = Enum.map(clients, &elem(&1, 1))
    expected = expected(survivors_clients, checkpointed)

    assert :ok = converged(survivors, doc_id, survivors_clients, expected),
           "seed #{seed}, killed #{victim}, paused #{paused}"

    assert Peers.snapshots(hd(survivors), doc_id) > 0
  end

  # The shards may still be moving into place just after the nodes start.
  defp owner(node, doc_id, attempts \\ 50) do
    case Peers.owner(node, doc_id) do
      {:error, _} when attempts > 0 ->
        receive after: (100 -> :ok)
        owner(node, doc_id, attempts - 1)

      owner ->
        owner
    end
  end

  defp edit(clients), do: clients |> Enum.random() |> elem(1) |> Client.edit()

  defp checkpointed(client) do
    case Client.checkpoint(client) do
      {:ok, tokens} -> tokens
      _ -> []
    end
  end

  defp expected(clients, checkpointed) do
    %{
      must: MapSet.new(Enum.flat_map(clients, &Client.tokens/1) ++ checkpointed),
      awareness: clients |> Enum.map(&Client.awareness_id/1) |> Enum.sort()
    }
  end

  defp converged(nodes, doc_id, clients, expected, attempts \\ 60) do
    Peers.sync_all(nodes, doc_id)
    stored = Peers.stored_text(hd(nodes), doc_id)
    texts = Enum.map(clients, &Client.text/1)
    tokens = String.split(stored, " ", trim: true)
    awareness = Enum.map(nodes, &Peers.awareness_ids(&1, doc_id))
    lost = expected.must |> MapSet.difference(MapSet.new(tokens)) |> Enum.sort()

    problems =
      [
        texts_differ: Enum.all?(texts, &(&1 == stored)),
        duplicates: length(tokens) == length(Enum.uniq(tokens)),
        foreign_tokens: Enum.all?(tokens, &(&1 =~ ~r/^c\d\d\.\d+$/)),
        lost_tokens: lost == [],
        awareness: Enum.all?(awareness, &(&1 == expected.awareness))
      ]
      |> Enum.reject(&elem(&1, 1))
      |> Enum.map(&elem(&1, 0))

    cond do
      problems == [] ->
        :ok

      attempts == 0 ->
        {:error,
         %{
           problems: problems,
           lost: Enum.take(lost, 10),
           stored_bytes: byte_size(stored),
           client_bytes: Enum.map(texts, &byte_size/1),
           awareness: awareness,
           expected_awareness: expected.awareness
         }}

      true ->
        receive after: (250 -> :ok)
        converged(nodes, doc_id, clients, expected, attempts - 1)
    end
  end
end
