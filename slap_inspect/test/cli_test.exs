defmodule Slap.Inspect.CLITest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Slap.Inspect.{
    Bytes,
    CLI,
    Clusters,
    Command,
    Feedback,
    JSON,
    Labels,
    Operation,
    Options,
    Query,
    Remote,
    Request
  }

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

  test "the terminal starts on home unless a cluster, command or query option is given" do
    connection = Enum.take(@connection, 4)

    assert {:ok, _endpoint, nil, %{view: :home}, %{json: false}} = CLI.parse(connection)

    assert {:ok, _endpoint, nil, %{view: :home}, %{refresh: 1000}} =
             CLI.parse(connection ++ ["--refresh", "1000"])

    assert {:ok, _endpoint, nil, %{view: :raw}, _mode} = CLI.parse(connection ++ ["--limit", "5"])

    assert {:ok, _endpoint, nil, %{view: :raw}, %{json: true}} =
             CLI.parse(["--json" | connection])

    assert {:ok, _endpoint, nil, %{view: :home}, %{json: true}} =
             CLI.parse(connection ++ ["--json", "home"])

    assert {:error, {:invalid, "home lists every cluster; omit --cluster"}} =
             CLI.parse(["home" | @connection])
  end

  test "the cookie file is optional, and the node's host selects long names" do
    assert {:ok, %{cookie_file: nil, name_domain: :shortnames}, :clusters} =
             CLI.parse(["--node", "slap@host", "clusters"])

    assert {:ok, %{cookie_file: "/cookie", name_domain: :longnames}, :clusters} =
             CLI.parse(["--node", "slap@127.0.0.1", "--cookie-file", "/cookie", "clusters"])

    assert {:ok, %{name_domain: :longnames}, :clusters} =
             CLI.parse(["--node", "slap@localhost", "--longnames", "clusters"])

    assert {:error, {:invalid, _message}} = CLI.parse(["--node", "slap", "clusters"])
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

  test "command selection ranks supplied options, missing options and definition order" do
    for {view, opts, operation} <- [
          {:yjs, [], :yjs_list},
          {:yjs, [base: "/d", sid: 1], :yjs_log},
          {:yjs, [sid: 1], :yjs_messages},
          {:yjs, [content: true, base: "/d"], :yjs_content},
          {:yjs_content, [], :yjs_content},
          {:log, [], :log_list},
          {:log, [content: true, base: "/l", component: :index], :log_content},
          {:log, [component: :index], :log_component},
          {:retention, [path: "/s"], :retention_metadata},
          {:retention, [], :retention},
          {:kv, [], :kv_partitions},
          {:kv, [partition: "p"], :kv}
        ] do
      assert {:ok, ^operation, _form} = Operation.normalize(view, opts)
    end

    assert {:error, :unknown_view} = Operation.normalize(:nothing, [])
  end

  test "command option lists contain only query options" do
    for command <- Operation.commands() do
      assert Options.query_options(command) -- Options.query_options() == []
    end
  end

  test "command forms reject unused options" do
    for {args, message} <- [
          {["kv", "--path", "/x", "--partition", "p"], "kv --partition does not take --path"},
          {["kv", "--key-prefix", "a"], "kv does not take --key-prefix without --partition"},
          {["--root", "text:a"], "scan does not take --root"},
          {["streams", "--content"], "streams does not take --content"},
          {["files", "--key-prefix", "a"],
           "files does not take --key-prefix without --partition"},
          {["log", "--from", "5"], "log does not take --from without --base and --component"},
          {["log", "--base", "/b", "--from", "5"],
           "log --base does not take --from without --component"},
          {["log", "--content", "--base", "/b", "--component", "index"],
           "log --content --base does not take --component"},
          {["yjs", "--sid", "1", "--root", "text:text"],
           "yjs --sid does not take --root without --content and --base"},
          {["stream", "--path", "/x", "--expected-sid", "1"],
           "stream --path does not take --expected-sid"},
          {["log", "--content", "--base", "/x", "--limit", "1", "--cursor", "61"],
           "log --content --base does not take --limit, --cursor"},
          {["get", "--key", "61", "--limit", "1"], "get --key does not take --limit"},
          {["files", "--max-value-bytes", "5"],
           "files does not take --max-value-bytes without --partition"}
        ] do
      assert {:error, {:unsupported_options, _, _, _} = reason} = CLI.parse_query(args)
      assert Feedback.error(reason) == message
    end

    assert {:ok, _, _, %{view: :kv}, _} =
             CLI.parse(@connection ++ ["--json", "kv", "--partition", "p", "--limit", "5"])
  end

  test "programmatic queries reject missing arguments before connecting" do
    for {view, opts, required} <- [
          {:log, [component: "index"], [[:base, :component]]},
          {:yjs, [content: true], [[:base]]},
          {:logical, [], [[:path]]}
        ] do
      assert {:error, {:required_options, ^required}} =
               Query.execute(%{}, %{view: view, shard: nil, budget: 5000, opts: opts})
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

  test "JSON listings read smaller pages unless the command sets their size" do
    assert {:ok, _, _, %{opts: opts}, _} = CLI.parse(@connection ++ ["--json", "scan"])
    assert {opts[:limit], opts[:target_bytes]} == {50, 256 * 1024}

    assert {:ok, _, _, %{opts: opts}, _} =
             CLI.parse(@connection ++ ["--json", "scan", "--limit", "200"])

    assert {opts[:limit], opts[:target_bytes]} == {200, 256 * 1024}

    for args <- [
          ["scan"],
          ["--json", "get", "--key", "61"],
          ["--json", "log", "--base", "/l"],
          ["--json", "yjs", "--base", "/d"]
        ] do
      assert {:ok, _, _, %{opts: opts}, _} = CLI.parse(@connection ++ args)
      refute Keyword.has_key?(opts, :limit)
      refute Keyword.has_key?(opts, :target_bytes)
    end
  end

  test "the next page's JSON command runs in a shell and reads from the page's cursor" do
    {:ok, endpoint, cluster, request, _mode} =
      CLI.parse(
        ["--node", "slap@localhost", "--longnames", "--cookie-file", "/my cookie's"] ++
          ["--cluster", "Slap.KV.Cluster", "--json", "kv", "--partition", "it's users"]
      )

    assert Request.next(request, %{cursor: nil}) == nil
    next = Request.next(request, %{cursor: <<0, 255>>, cursor_exclusive: true})
    command = CLI.shell_command(endpoint, cluster, next)

    assert {output, 0} = System.cmd("sh", ["-c", "printf '%s\\0' " <> command])
    ["slap-inspect" | args] = output |> String.split(<<0>>) |> Enum.drop(-1)

    assert {:ok, ^endpoint, ^cluster, restored, %{json: true}} = CLI.parse(args)
    assert Enum.sort(restored.opts) == Enum.sort(next.opts)
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

  test "JSON encodes nonprintable map keys before escaping a leading dollar" do
    value = %{
      <<36, 255>> => "invalid UTF-8",
      <<36, 0>> => "nonprintable UTF-8",
      "$name" => "printable",
      "$bytes:24ff" => "literal tag",
      "nested" => [%{<<36, 255>> => "nested value"}]
    }

    assert Elixir.JSON.decode!(JSON.encode(value)) == %{
             "$bytes:24ff" => "invalid UTF-8",
             "$bytes:2400" => "nonprintable UTF-8",
             "$$name" => "printable",
             "$$bytes:24ff" => "literal tag",
             "nested" => [%{"$bytes:24ff" => "nested value"}]
           }
  end

  test "JSON distinguishes text, binary data and page cursors" do
    page = %{
      cursor: "/next",
      path: "/demo/events",
      value: <<255, 0, 27>>,
      error: :busy,
      metadata: %{"$user" => "a", <<255>> => "b"},
      content: {:json, ~s({"text":"hello"})}
    }

    assert Elixir.JSON.decode!(JSON.encode(page)) == %{
             "cursor" => "2f6e657874",
             "path" => "/demo/events",
             "value" => %{"$bytes" => "ff001b"},
             "error" => "busy",
             "metadata" => %{"$$user" => "a", "$bytes:ff" => "b"},
             "content" => %{"text" => "hello"}
           }
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

  test "hex dumps adapt byte counts and spacing to the available width" do
    bytes = :binary.list_to_bin(Enum.to_list(0..15))

    spaced16 =
      "00000000  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F  " <> String.duplicate(".", 16)

    grouped16 = "00000000  00010203 04050607 08090A0B 0C0D0E0F  " <> String.duplicate(".", 16)
    spaced8 = "00000000  00 01 02 03 04 05 06 07  ........"
    grouped8 = "00000000  00010203 04050607  ........"
    spaced4 = "00000000  00 01 02 03  ...."

    for {width, line} <- [
          {80, spaced16},
          {75, spaced16},
          {74, grouped16},
          {63, grouped16},
          {62, spaced8},
          {43, spaced8},
          {42, grouped8},
          {37, grouped8},
          {36, spaced4},
          {10, spaced4}
        ] do
      assert [^line | _] = String.split(Bytes.hex(bytes, 0, width), "\n")
      assert String.length(line) <= max(width, 27)
    end

    assert Bytes.hex(<<1, 2, 3, 4, 5>>, 0, 63) ==
             "00000000  01020304 05" <> String.duplicate(" ", 24) <> "  ....."
  end

  test "byte rendering escapes terminal controls" do
    refute Bytes.text(<<27, ?[, ?3, ?1, ?m>>) =~ <<27>>
    refute Bytes.hex(<<27, 255, 10>>, 0, 80) =~ <<27>>
    assert Bytes.hex("abc", 16, 80) =~ "00000010"

    controls = "/a\e[2J\r\a\b\v\f\u0085"
    message = %{kind: :message, offset: 1, part: 0}

    for text <- [
          Bytes.name(controls),
          Bytes.display(controls),
          Labels.label(%{key: "", decoded: {:ok, %{path: controls}}}),
          Labels.label(%{key: "", decoded: {:ok, message}, value: controls, value_size: 13})
        ] do
      refute text =~ ~r/[[:cntrl:]]/u
    end

    assert Bytes.display("a\nb") == "a\nb"
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
