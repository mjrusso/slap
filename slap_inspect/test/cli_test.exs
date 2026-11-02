defmodule Slap.Inspect.CLITest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Slap.Inspect.{Bytes, CLI, Clusters, Command, JSON, Query, Remote}

  @connection [
    "--node",
    "slap@localhost",
    "--cookie-file",
    "/cookie",
    "--cluster",
    "Slap.Streams.Cluster"
  ]

  test "executable help works without a connection" do
    for arg <- ["--help", "-h"] do
      output = capture_io(fn -> assert :ok = CLI.main([arg]) end)
      assert output =~ "Usage: slap-inspect"
      assert output =~ "--json"
    end
  end

  test "the cluster is optional, and the clusters command lists them" do
    connection = Enum.take(@connection, 4)

    assert {:ok, _connection, nil, %{view: :kv}, %{json: true}} =
             CLI.parse(connection ++ ["--json", "kv"])

    assert {:ok, _connection, Slap.KV.Cluster, %{view: :raw}, _mode} =
             CLI.parse(connection ++ ["--cluster", "Slap.KV.Cluster"])

    assert {:ok, _endpoint, :clusters} = CLI.parse(["clusters" | connection])
    assert {:error, {:required, :cluster}} = CLI.parse(connection ++ ["--cluster", ""])
    assert {:error, {:invalid, _message}} = CLI.parse(["clusters" | @connection])
  end

  test "JSON output reads the one cluster that serves its view, and names the candidates otherwise" do
    streams = %{cluster: Demo.Streams, services: [:streams]}
    kv = %{cluster: Demo.KV, services: [:kv]}
    plain = %{cluster: Demo.Plain, services: []}

    assert {:ok, Demo.KV} = Clusters.only([streams, kv, plain], :files)
    assert {:ok, Demo.Streams} = Clusters.only([streams, kv, plain], :yjs_content)

    assert {:error, {:ambiguous_cluster, :raw, ["Demo.Streams", "Demo.KV"]}} =
             Clusters.only([streams, kv], :raw)

    assert {:error, {:no_cluster_for_view, :kv}} = Clusters.only([streams, plain], :kv)
    assert {:error, :no_clusters} = Clusters.only([], :raw)
  end

  test "executable errors write JSON to stderr and exit nonzero" do
    assert capture_io(fn ->
             error =
               capture_io(:stderr, fn ->
                 assert catch_exit(CLI.main(["--json"])) == {:shutdown, 1}
               end)

             assert Elixir.JSON.decode!(error) == %{"error" => ["required", "node"]}
           end) == ""
  end

  test "executable errors keep messages and option names readable" do
    for {args, expected} <- [
          {@connection ++ ["--json", "--invalid", "x"], ["invalid_options", ["--invalid"]]},
          {@connection ++ ["--json", "messages", "--sid", "1", "--from", "5", "--until", "1"],
           ["invalid", "--until must be greater than or equal to --from"]}
        ] do
      error = capture_io(:stderr, fn -> assert catch_exit(CLI.main(args)) == {:shutdown, 1} end)
      assert Elixir.JSON.decode!(error) == %{"error" => expected}
    end
  end

  test "hex filters reject incomplete pairs and control sequences" do
    assert {:ok, _, _, %{opts: opts}, _} =
             CLI.parse(@connection ++ ["--json", "scan", "--prefix", "00 ff 01"])

    assert opts[:prefix] == <<0, 255, 1>>

    for value <- ["0", "00 0", "0 0", "gg", "00\nff", "\e[31m"] do
      assert {:error, {:invalid_hex, :prefix}} =
               CLI.parse(@connection ++ ["--json", "scan", "--prefix", value])
    end
  end

  test "commands retain opaque bytes and reject unknown switches" do
    assert {:ok, _, _, %{view: :kv, opts: opts}, %{json: true}} =
             CLI.parse(
               @connection ++ ["--json", "kv", "--partition", "users", "--key-prefix", "ff"]
             )

    assert opts[:key_prefix] == <<255>>
    assert {:error, _} = CLI.parse(@connection ++ ["--invalid", "x"])
    assert {:error, _} = CLI.parse(["stream" | @connection])
  end

  test "numeric ranges are rejected before connection in both entry points" do
    for args <- [
          ["get", "--key", "61", "--offset", "-1"],
          ["get", "--key", "61", "--length", "-1"],
          ["object", "--object-key", "key", "--length", "0"],
          ["scan", "--limit", "0"],
          ["scan", "--refresh", "4294967296"],
          ["scan", "--max-key-bytes", "0"],
          ["scan", "--max-value-bytes", "0"],
          ["yjs", "--content", "--base", "/doc", "--max-entries", "0"],
          ["messages", "--sid", "-1"]
        ],
        mode <- [[], ["--json"]] do
      assert {:error, {:invalid_option, _, _, _}} = CLI.run(@connection ++ mode ++ args)
    end

    assert {:ok, _, _, _, _} =
             CLI.parse(@connection ++ ["get", "--key", "61", "--offset", "0", "--length", "0"])
  end

  test "programmatic queries reject missing arguments before connecting" do
    for {view, required} <- [log: [:base], yjs: [:sid], logical: [:path]] do
      assert {:error, {:required_options, ^required}} =
               Query.execute(%{}, %{view: view, shard: nil, budget: 5000, opts: []})
    end
  end

  test "default object ranges respect a reduced preview limit" do
    assert {:ok, _, _, %{opts: opts}, _} =
             CLI.parse(
               @connection ++ ["object", "--object-key", "key", "--max-value-bytes", "1024"]
             )

    assert opts[:value_range] == {0, 1024}
  end

  test "editable commands preserve quoted values, byte filters and root choices" do
    for args <- [
          [
            "kv",
            "--partition",
            "a \"quoted\" \\ partition\\",
            "--key-prefix",
            "00ff",
            "--shard",
            "2"
          ],
          ["get", "--key", "", "--offset", "3", "--length", "10", "--budget", "9000"],
          [
            "yjs",
            "--content",
            "--base",
            "/my doc",
            "--root",
            "text:a:b",
            "--root",
            "map:settings"
          ],
          ["scan", "--cursor", "00", "--cursor-exclusive"],
          ["logical", "--path", "/fork", "--expected-sid", "12", "--shard", "0"],
          ["logical", "--path", <<255>>]
        ] do
      assert {:ok, _, _, request, _} = CLI.parse(@connection ++ args)
      {:ok, command} = request |> CLI.command() |> Command.split()
      assert {:ok, _, _, restored, _} = CLI.parse(@connection ++ command)
      assert Map.drop(restored, [:opts]) == Map.drop(request, [:opts])
      assert Enum.sort(restored.opts) == Enum.sort(request.opts)
    end
  end

  test "editable queries reject connection and process options" do
    for option <- ["--cluster", "--node", "--cookie-file", "--refresh"] do
      assert {:error, _} = CLI.parse_query(["scan", option, "another"])
    end

    for option <- ["--json", "--longnames"] do
      assert {:error, _} = CLI.parse_query(["scan", option])
    end

    assert {:ok, %{view: :kv, opts: opts}} = CLI.parse_query(["kv", "--partition", "users"])
    assert opts[:partition] == "users"
  end

  test "query variants normalize into explicit operations and round-trip" do
    for {args, operation} <- [
          {["yjs", "--sid", "7"], :yjs_messages},
          {["yjs", "--base", "/doc"], :yjs_log},
          {["yjs", "--content", "--base", "/doc"], :yjs_content},
          {["log", "--base", "/log"], :log_catalog},
          {["log", "--base", "/log", "--component", "index"], :log_component},
          {["retention", "--sid", "7"], :retention},
          {["retention", "--sid", "7", "--path", "/stream"], :retention_metadata}
        ] do
      assert {:ok, %{operation: ^operation} = request} = CLI.parse_query(args)
      assert {:ok, tokens} = request |> CLI.command() |> Command.split()
      assert {:ok, restored} = CLI.parse_query(tokens)
      assert restored.operation == request.operation
      assert restored.view == request.view
      assert Enum.sort(restored.opts) == Enum.sort(request.opts)
    end
  end

  test "JSON encodes binaries as hex and status atoms as text" do
    assert %{"ff" => "00"} = Elixir.JSON.decode!(JSON.encode(%{<<255>> => <<0>>}))

    assert %{"value" => "ff001b", "error" => "busy"} =
             Elixir.JSON.decode!(JSON.encode(%{value: <<255, 0, 27>>, error: :busy}))

    assert %{"content" => %{"text" => "hello"}, "raw" => "68656c6c6f"} =
             Elixir.JSON.decode!(
               JSON.encode(%{content: {:json, ~s({"text":"hello"})}, raw: "hello"})
             )
  end

  test "content commands parse root interpretations and history offsets" do
    assert {:ok, _, _, %{view: :yjs_content, budget: 10_000, opts: opts}, _} =
             CLI.parse(
               @connection ++
                 [
                   "yjs",
                   "--content",
                   "--base",
                   "/doc",
                   "--root",
                   "text:body",
                   "--root",
                   "map:settings",
                   "--at-offset",
                   "12",
                   "--max-input-bytes",
                   "4096"
                 ]
             )

    assert opts[:roots] == [{"body", :text}, {"settings", :map}]
    assert opts[:at_offset] == 12
    assert opts[:max_input_bytes] == 4096

    assert {:error, :invalid_root_interpretation} =
             CLI.parse(
               @connection ++ ["yjs", "--content", "--base", "/doc", "--root", "unknown:name"]
             )
  end

  test "byte rendering escapes terminal controls" do
    refute Bytes.text(<<27, ?[, ?3, ?1, ?m>>) =~ <<27>>
    refute Bytes.hex(<<27, 255, 10>>) =~ <<27>>
    assert Bytes.hex("abc", 16) =~ "00000010"
  end

  test "invalid cookie files return a sanitized error before distribution starts" do
    path =
      Path.join(System.tmp_dir!(), "inspect-invalid-cookie-#{System.unique_integer([:positive])}")

    File.write!(path, String.duplicate("secret", 100))
    on_exit(fn -> File.rm(path) end)

    assert {:error, :connection_failed} =
             Remote.connect(:unused@localhost, path, name_domain: :shortnames)
  end

  test "remote targets are rejected before connection" do
    assert {:error, :forbidden_target} =
             Remote.call(:unconnected@localhost, System, :cmd, ["false", []])

    assert {:error, :forbidden_target} =
             Remote.read(
               %{entry: :unconnected@localhost, cluster: Cluster},
               %{shard: 0, opts: []},
               {File, :read},
               System.monotonic_time(:millisecond) + 5000
             )
  end
end
