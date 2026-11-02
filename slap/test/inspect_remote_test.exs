defmodule Slap.InspectRemoteTest do
  # The test owns distributed Erlang and the named clusters on its peers.
  use ExUnit.Case, async: false

  import Slap.Test.InspectPeers

  alias Slap.{KV, SnapshotLog, Streams}
  alias Slap.Streams.Introspect
  alias Slap.Yjs.Frame

  @moduletag :capture_log
  @moduletag timeout: 120_000
  @remote Slap.Inspect.Remote
  @query Slap.Inspect.Query

  test "a hidden inspector reads owners directly and reports missing and incompatible owners" do
    distribute()
    {_entry_peer, entry} = peer("inspect-entry")
    {_owner_peer, owner} = peer("inspect-owner")
    {_inspector_peer, inspector} = peer("inspect-client", [~c"-hidden"])
    start_cluster(entry, Streams.Cluster, nodes: [entry, owner])
    start_cluster(owner, Streams.Cluster, nodes: [entry, owner])
    assert true = :erpc.call(entry, Node, :connect, [owner], 5000)

    for node <- [entry, owner],
        do: assert(:ok == :erpc.call(node, Streams.Cluster, :await_ready, [10_000], 15_000))

    path =
      Enum.find(Enum.map(1..100, &"/inspect-#{&1}"), fn path ->
        :erpc.call(entry, Slap.Cluster, :shard_for, [Streams.Cluster, path], 5000) == 1
      end)

    assert {:ok, _, _} =
             :erpc.call(entry, Streams, :create, [path, [body: <<0, 255, 27>>]], 10_000)

    assert {:ok, page} =
             read(inspector, entry, 1, :stream, path: path)

    assert page.owner == owner
    assert page.shard == 1
    assert %{decoded: {:ok, %{value: %{sid: sid}}}} = page.row

    assert {:ok, messages} =
             query(inspector, entry, Streams.Cluster, %{
               view: :messages,
               shard: 1,
               budget: 5000,
               opts: [sid: sid]
             })

    assert [%{value: <<0, 255, 27>>, complete: true}] = messages.messages

    target =
      Enum.find(Enum.map(1..100, &"/fork-#{&1}"), fn candidate ->
        :erpc.call(entry, Slap.Cluster, :shard_for, [Streams.Cluster, candidate], 5000) == 0
      end)

    assert {:ok, _, _} =
             :erpc.call(
               entry,
               Streams,
               :create,
               [target, [forked_from: path, body: "own"]],
               10_000
             )

    assert {:ok, fork} =
             query(inspector, entry, Streams.Cluster, %{
               view: :logical,
               shard: 0,
               budget: 5000,
               opts: [path: target],
               observer: self()
             })

    assert [%{value: <<0, 255, 27>>, complete: true}] = fork.messages
    assert [%{capture: %{source: %{shard: 1, sid: ^sid}}}] = fork.lineage
    assert_receive {:inspection_owner, ^entry, 0}
    assert_receive {:inspection_owner, ^owner, 1}
    assert_receive {:inspection_owner, ^owner, 1}

    assert {:ok, own} =
             query(inspector, entry, Streams.Cluster, %{
               view: :logical,
               shard: 0,
               budget: 5000,
               opts: [path: target, cursor: fork.cursor],
               observer: self()
             })

    assert [%{value: "own", complete: true}] = own.messages
    assert_receive {:inspection_owner, ^entry, 0}
    assert_receive {:inspection_owner, ^entry, 0}

    doc = Yex.Doc.new()
    :ok = Yex.Text.insert(Yex.Doc.get_text(doc, "body"), 0, "remote content")
    framed = Frame.frame(Yex.encode_state_as_update!(doc))
    assert {:ok, tail} = :erpc.call(entry, SnapshotLog, :append, [path, framed], 10_000)
    assert {:ok, _} = :erpc.call(owner, Application, :ensure_all_started, [:slap_yjs], 10_000)

    assert {:ok, %{owner: ^owner, content: {:json, json}, coverage: %{until: ^tail}}} =
             query(inspector, entry, Streams.Cluster, %{
               view: :yjs_content,
               shard: 1,
               budget: 10_000,
               opts: [base: path, roots: [{"body", :text}]]
             })

    assert [%{"value" => "remote content"}] = JSON.decode!(json)["roots"]

    assert :ok =
             :erpc.call(
               entry,
               SnapshotLog,
               :snapshot,
               [path, tail, Yex.encode_state_as_update!(doc)],
               10_000
             )

    fork_base =
      Enum.find(Enum.map(1..100, &"/snapshot-fork-#{&1}"), fn candidate ->
        :erpc.call(entry, Slap.Cluster, :shard_for, [Streams.Cluster, candidate], 5000) == 0
      end)

    assert {:ok, _} = :erpc.call(entry, SnapshotLog, :fork, [path, fork_base], 10_000)
    assert {:ok, _} = :erpc.call(entry, Application, :ensure_all_started, [:slap_yjs], 10_000)

    assert {:ok,
            %{owner: ^entry, content: {:json, fork_json}, coverage: %{snapshot_offset: ^tail}}} =
             query(inspector, entry, Streams.Cluster, %{
               view: :yjs_content,
               shard: 0,
               budget: 10_000,
               opts: [base: fork_base, roots: [{"body", :text}]]
             })

    assert [%{"value" => "remote content"}] = JSON.decode!(fork_json)["roots"]
    assert false == :erpc.call(inspector, :code, :is_loaded, [Yex.Doc], 5000)

    assert {:error, :not_owner} =
             :erpc.call(entry, Introspect, :scan, [Streams.Cluster, 1, 1, 5000, []], 10_000)

    assert {:error, :unassigned} =
             read(inspector, entry, 9, :list, [])

    assert :ok = :erpc.call(owner, Streams.Cluster, :stop, [], 30_000)

    assert {:error, :not_owner} =
             read(inspector, entry, 1, :list, [])

    assert {true, _} =
             eval(
               owner,
               ":code.purge(Slap.Streams.Introspect); :code.delete(Slap.Streams.Introspect)"
             )

    assert {:ok, _} =
             eval(
               owner,
               "Code.compiler_options(ignore_module_conflict: true); Code.compile_string(\"defmodule Slap.Streams.Introspect do def list(_, _, _, _, _), do: {:error, {:unsupported_api, 2}} end\"); :ok"
             )

    assert {:error, {:unsupported_api, 2}} =
             read(inspector, entry, 1, :list, [])

    assert {:error, :owner_does_not_support_inspection} =
             read(inspector, entry, 1, :messages, sid: 1)
  end

  test "an entry with no local shards lists its clusters, and reads go to their owner" do
    distribute()
    {_entry_peer, entry} = peer("inspect-discovery-entry")
    {_owner_peer, owner} = peer("inspect-discovery-owner")
    {_inspector_peer, inspector} = peer("inspect-discovery-client", [~c"-hidden"])
    assert true = :erpc.call(entry, Node, :connect, [owner], 5000)

    nodes_and_clusters =
      for node <- [entry, owner], cluster <- [Streams.Cluster, KV.Cluster], do: {node, cluster}

    # The entry's clusters are ready only once the owner has opened their shards.
    for {node, cluster} <- nodes_and_clusters,
        do: start_cluster(node, cluster, assignments: %{owner => [0..1]})

    for {node, cluster} <- nodes_and_clusters,
        do: assert(:ok == :erpc.call(node, cluster, :await_ready, [10_000], 15_000))

    assert {:ok,
            [
              %{cluster: KV.Cluster, shards: 2, local_shards: [], services: [:kv]},
              %{cluster: Streams.Cluster, shards: 2, local_shards: [], services: [:streams]}
            ]} = :erpc.call(inspector, Slap.Inspect.Clusters, :discover, [entry])

    streams = %{view: :streams, shard: 0, budget: 5000, opts: []}
    kv = %{view: :kv, shard: nil, budget: 5000, opts: [partition: "users"]}
    assert {:ok, %{owner: ^owner}} = query(inspector, entry, Streams.Cluster, streams)
    assert {:ok, %{owner: ^owner}} = query(inspector, entry, KV.Cluster, kv)
  end

  test "an unnamed inspector starts a hidden connection from its cookie file" do
    distribute()
    {_entry_peer, entry} = peer("inspect-connection")
    cookie_path = Path.join(tmp_dir(), "cookie")
    File.write!(cookie_path, Atom.to_string(Node.get_cookie()))

    inspector = connect_unnamed(entry, cookie_path, %{})
    refute inspector in :erpc.call(entry, Node, :list, [], 5000)
    assert inspector in :erpc.call(entry, Node, :list, [:hidden], 5000)
  end

  test "an unnamed inspector without a cookie file uses Erlang's default cookie file" do
    distribute()
    {_entry_peer, entry} = peer("inspect-default-cookie")
    home = tmp_dir()
    cookie_path = Path.join(home, ".erlang.cookie")
    File.write!(cookie_path, Atom.to_string(Node.get_cookie()))
    # Erlang rejects a cookie file that others can read.
    File.chmod!(cookie_path, 0o600)

    inspector = connect_unnamed(entry, nil, %{env: [{~c"HOME", String.to_charlist(home)}]})
    assert inspector in :erpc.call(entry, Node, :list, [:hidden], 5000)
  end

  defp tmp_dir do
    path = Path.join(System.tmp_dir!(), "inspect-remote-#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf(path) end)
    path
  end

  defp connect_unnamed(entry, cookie_file, peer_options) do
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])
    options = Map.merge(%{connection: :standard_io, args: [~c"+S", ~c"2" | paths]}, peer_options)
    {:ok, client, _node} = :peer.start(options)
    on_exit(fn -> :peer.stop(client) end)
    assert {:ok, _} = :peer.call(client, :application, :ensure_all_started, [:elixir], 10_000)

    assert :ok =
             :peer.call(
               client,
               @remote,
               :connect,
               [entry, cookie_file, [name_domain: :longnames]],
               10_000
             )

    :peer.call(client, Node, :self, [], 5000)
  end

  # Remote.read takes a deadline in the inspector's monotonic clock.
  defp read(inspector, entry, shard, function, opts) do
    deadline = :erpc.call(inspector, System, :monotonic_time, [:millisecond]) + 5000

    :erpc.call(
      inspector,
      @remote,
      :read,
      [
        %{entry: entry, cluster: Streams.Cluster},
        %{shard: shard, opts: opts},
        {Introspect, function},
        deadline
      ],
      10_000
    )
  end

  defp query(inspector, entry, cluster, request) do
    connection = %{entry: entry, cluster: cluster}
    :erpc.call(inspector, @query, :execute, [connection, request], request.budget + 5000)
  end

  defp start_cluster(node, cluster, static_opts) do
    assert {:ok, _} =
             :erpc.call(
               node,
               Application,
               :ensure_all_started,
               [[:slap_streams, :slap_kv]],
               30_000
             )

    opts = [
      store: :memory,
      shards: 2,
      settings: %{flush_interval: "2ms"},
      strategy: {Slap.Cluster.Strategy.Static, static_opts}
    ]

    assert {:ok, _} =
             eval(
               node,
               "{:ok, pid} = cluster.start_link(opts); Process.unlink(pid); :ok",
               cluster: cluster,
               opts: opts
             )
  end
end
