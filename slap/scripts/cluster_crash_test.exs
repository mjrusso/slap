# Faults on a cluster under load. Each run starts a cluster of
# `mix slap.server` nodes (separate OS processes, one store), has writers
# append through the nodes, injects a
# fault into one node at a random moment, lets the cluster recover, and
# reads every stream back to check that:
#
#   * every acknowledged append is there, exactly once, at the offset its
#     acknowledgement gave (so no offset was reused after the failover);
#   * no message appears twice, and each writer's messages are in order;
#   * a producer's retries of appends that failed are applied once.
#
# The faults, taken in turn (or one with --fault):
#
#   * kill  - SIGKILL the node; it is restarted after the failover.
#   * pause - SIGSTOP the node past its leases (or net_ticktime); the others
#             take (and fence) its shards. Then SIGCONT.
#
# For kill, it also measures the recovery time: the longest time any shard
# could not be written, appending to one stream per shard every 100 ms.
#
#     mix run scripts/cluster_crash_test.exs [--runs 6] [--nodes 3]
#         [--shards 64] [--writers 16] [--fault kill|pause]
#         [--store local | s3:s3://bucket/prefix] [--lease-ttl 5000]
#         [--placement distributed|object-lease]
#         [--base-port 4550]
#
# object-lease needs an S3 store (the local file system has no If-Match).

Code.require_file("../bench/support/raw_http.exs", __DIR__)
alias Slap.Bench.RawHTTP

{opts, _} =
  OptionParser.parse!(System.argv(),
    strict: [
      runs: :integer,
      nodes: :integer,
      shards: :integer,
      writers: :integer,
      fault: :string,
      store: :string,
      lease_ttl: :integer,
      placement: :string,
      base_port: :integer
    ]
  )

config = %{
  runs: Keyword.get(opts, :runs, 6),
  nodes: Keyword.get(opts, :nodes, 3),
  shards: Keyword.get(opts, :shards, 64),
  writers: Keyword.get(opts, :writers, 16),
  placement: Keyword.get(opts, :placement, "distributed"),
  faults: if(f = opts[:fault], do: [f], else: ~w(kill pause)),
  store: Keyword.get(opts, :store, "local"),
  ttl: Keyword.get(opts, :lease_ttl, 5_000),
  base_port: Keyword.get(opts, :base_port, 4550),
  tmp: Path.join(System.tmp_dir!(), "slap-cluster-crash-#{System.os_time(:millisecond)}"),
  elixir: System.find_executable("elixir"),
  project: Path.expand("..", __DIR__)
}

defmodule Cluster do
  def port(c, i), do: c.base_port + i

  defp names(c), do: Enum.map_join(1..c.nodes, ",", &"n#{&1}@127.0.0.1")

  def start_node(c, i, store) do
    pid_file = Path.join(c.tmp, "n#{i}.pid")
    File.rm(pid_file)

    placement =
      case c.placement do
        "object-lease" -> ~w(--placement object-lease --lease-ttl #{c.ttl})
        # A paused node is noticed after net_ticktime.
        "distributed" -> ~w(--placement distributed)
      end

    vm = if c.placement == "distributed", do: ["--erl", "-kernel net_ticktime 4"], else: []

    args =
      vm ++
        ~w(--name n#{i}@127.0.0.1 --cookie slap-crash -S mix slap.server --streams --port #{port(c, i)}
         --store #{store} --streams-shards #{c.shards} --streams-flush-interval 10ms) ++
        placement ++ ~w(--peers #{names(c)} --pid-file #{pid_file})

    log = File.open!(Path.join(c.tmp, "n#{i}.log"), [:append])

    port =
      Port.open({:spawn_executable, c.elixir}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        cd: c.project,
        args: args
      ])

    spawn(fn -> drain(port, log) end) |> then(&Port.connect(port, &1))
    wait_file(pid_file, 600)
    os_pid = pid_file |> File.read!() |> String.trim()
    Agent.update(:started_nodes, &[os_pid | &1])
    %{port: port, os_pid: os_pid, i: i}
  end

  defp drain(port, log) do
    receive do
      {^port, {:data, data}} ->
        IO.binwrite(log, data)
        drain(port, log)

      {^port, {:exit_status, _}} ->
        File.close(log)
    end
  end

  defp wait_file(_file, 0), do: raise("a node did not start")

  defp wait_file(file, n) do
    if File.exists?(file) and File.read!(file) != "",
      do: :ok,
      else: Process.sleep(100) && wait_file(file, n - 1)
  end

  def signal(node, sig), do: System.cmd("kill", ["-#{sig}", node.os_pid])

  def alive?(node),
    do: match?({_, 0}, System.cmd("kill", ["-0", node.os_pid], stderr_to_stdout: true))

  def path(run, name), do: "/v1/stream/crash#{run}/#{name}"

  def probe_streams(c, run) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(&path(run, "probe#{&1}"))
    |> Enum.reduce_while(%{}, fn p, acc ->
      acc = Map.put_new(acc, Slap.Cluster.Hash.shard_for(p, c.shards), p)
      if map_size(acc) == c.shards, do: {:halt, acc}, else: {:cont, acc}
    end)
    |> Map.values()
  end

  def create_all(port, paths, tries \\ 600) do
    remaining =
      Enum.reject(paths, fn p ->
        response = RawHTTP.request_once(port, "PUT", p, [{"content-type", "application/json"}])
        match?({:ok, code, _, _} when code in [200, 201], response)
      end)

    cond do
      remaining == [] -> :ok
      tries == 0 -> raise "could not create #{length(remaining)} streams"
      true -> Process.sleep(100) && create_all(port, remaining, tries - 1)
    end
  end

  def longest_write_outage(port, streams, until) do
    streams
    |> Task.async_stream(&probe_stream(port, &1, until, nil, 0),
      max_concurrency: length(streams),
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, ms} -> ms end)
    |> Enum.max()
  end

  defp probe_stream(port, path, until, down_since, worst) do
    if System.monotonic_time(:millisecond) >= until do
      if down_since, do: max(worst, until - down_since), else: worst
    else
      response =
        RawHTTP.request_once(port, "POST", path, [{"content-type", "application/json"}], "0")

      ok = match?({:ok, 204, _, _}, response)

      now = System.monotonic_time(:millisecond)

      {down_since, worst} =
        cond do
          ok and down_since -> {nil, max(worst, now - down_since)}
          ok -> {nil, worst}
          down_since -> {down_since, worst}
          true -> {now, worst}
        end

      Process.sleep(100)
      probe_stream(port, path, until, down_since, worst)
    end
  end
end

defmodule Writer do
  # Appends to its own stream through the nodes until the deadline. A plain
  # writer gives up on an append that failed (it is in doubt: stored or
  # not); a producer retries it until it is acknowledged, which must apply
  # it once. Returns the acknowledgements, {n, next_offset | :duplicate}.
  def run(c, run, i, deadline) do
    state = %{
      c: c,
      path: Cluster.path(run, "w#{i}"),
      i: i,
      producer: rem(i, 2) == 0,
      node: rem(i, c.nodes) + 1,
      socket: nil,
      n: 0,
      acks: [],
      deadline: deadline
    }

    loop(state)
  end

  defp loop(%{deadline: deadline} = s) do
    if System.monotonic_time(:millisecond) >= deadline do
      if s.socket, do: RawHTTP.close(s.socket)
      Enum.reverse(s.acks)
    else
      s |> step() |> loop()
    end
  end

  defp step(s) do
    s = connect(s)

    headers =
      [{"content-type", "application/json"}] ++
        if s.producer,
          do: [{"producer-id", "w#{s.i}"}, {"producer-epoch", "0"}, {"producer-seq", "#{s.n}"}],
          else: []

    case s.socket && RawHTTP.request(s.socket, "POST", s.path, headers, ~s("w#{s.i}-#{s.n}")) do
      {:ok, status, h, _} when status in [200, 204] ->
        ack =
          if s.producer and status == 204,
            do: :duplicate,
            else: offset(RawHTTP.header(h, "stream-next-offset"))

        %{s | acks: [{s.n, ack} | s.acks], n: s.n + 1}

      {:ok, 503, _, _} ->
        Process.sleep(50)
        # The request may or may not have been applied.
        if s.producer, do: s, else: %{s | n: s.n + 1}

      {:ok, status, _, body} ->
        raise "w#{s.i}-#{s.n}: #{status} #{body}"

      _ ->
        if s.socket, do: RawHTTP.close(s.socket)
        Process.sleep(50)
        next = rem(s.node, s.c.nodes) + 1
        s = %{s | socket: nil, node: next}
        if s.producer, do: s, else: %{s | n: s.n + 1}
    end
  end

  defp connect(%{socket: nil} = s) do
    case RawHTTP.connect(Cluster.port(s.c, s.node)) do
      {:ok, socket} -> %{s | socket: socket}
      {:error, _} -> s
    end
  end

  defp connect(s), do: s

  defp offset(wire) do
    {:ok, o} = Slap.Streams.Offset.parse(wire)
    o
  end
end

defmodule Verify do
  defp read_all(port, path) do
    {:ok, s} = RawHTTP.connect(port)
    messages = read_from(s, path, "-1", 0, [])
    RawHTTP.close(s)
    messages
  end

  defp read_from(s, path, wire, at, acc, tries \\ 200) do
    case RawHTTP.request(s, "GET", "#{path}?offset=#{wire}") do
      {:ok, 503, _, _} when tries > 0 ->
        Process.sleep(100)
        read_from(s, path, wire, at, acc, tries - 1)

      {:ok, 200, h, body} ->
        read_page(s, path, at, acc, h, body)
    end
  end

  defp read_page(s, path, at, acc, h, body) do
    {:ok, values} = JSON.decode(body)

    {placed, at} =
      Enum.map_reduce(values, at, fn v, at ->
        size = byte_size(JSON.encode!(v))
        {{v, at, at + 4 + size}, at + 4 + size}
      end)

    acc = acc ++ placed

    if RawHTTP.header(h, "stream-up-to-date") == "true",
      do: acc,
      else: read_from(s, path, RawHTTP.header(h, "stream-next-offset"), at, acc)
  end

  def check(port, path, i, acks) do
    messages = read_all(port, path)
    by_body = Enum.group_by(messages, fn {v, _, _} -> v end)

    bad =
      for {n, ack} <- acks,
          found = Map.get(by_body, "w#{i}-#{n}", []),
          not match?([_], found) or (ack != :duplicate and elem(hd(found), 2) != ack),
          do: {"w#{i}-#{n}", ack, found}

    ns =
      for {v, _, _} <- messages, do: v |> String.split("-") |> List.last() |> String.to_integer()

    %{
      acked: length(acks),
      dupes: for({v, l} <- by_body, length(l) > 1, do: v),
      bad: bad,
      ordered: ns == Enum.sort(ns)
    }
  end
end

File.mkdir_p!(config.tmp)
{:ok, _} = Agent.start_link(fn -> [] end, name: :started_nodes)

# Whatever happens, no node outlives the script.
kill_all = fn ->
  for os_pid <- Agent.get(:started_nodes, & &1),
      do: System.cmd("kill", ["-9", os_pid], stderr_to_stdout: true)
end

results =
  try do
    for run <- 1..config.runs do
      c = config
      fault = Enum.at(c.faults, rem(run - 1, length(c.faults)))
      victim = :rand.uniform(c.nodes)

      store =
        case c.store do
          "local" -> "local:#{c.tmp}/run-#{run}"
          "s3:" <> url -> "s3:#{url}/run-#{run}-#{System.os_time(:millisecond)}"
        end

      nodes = for i <- 1..c.nodes, into: %{}, do: {i, Cluster.start_node(c, i, store)}

      probes = Cluster.probe_streams(c, run)
      Cluster.create_all(Cluster.port(c, 1), probes)
      writer_paths = for i <- 1..c.writers, do: Cluster.path(run, "w#{i}")
      Cluster.create_all(Cluster.port(c, 1), writer_paths)

      fault_at = 1_000 + :rand.uniform(2_000)
      start = System.monotonic_time(:millisecond)
      deadline = start + fault_at + 3 * c.ttl + 3_000

      writers =
        for i <- 1..c.writers, do: Task.async(fn -> Writer.run(c, run, i, deadline) end)

      Process.sleep(fault_at)
      survivor = Enum.find(1..c.nodes, &(&1 != victim))

      case fault do
        "kill" -> Cluster.signal(nodes[victim], "KILL")
        "pause" -> Cluster.signal(nodes[victim], "STOP")
      end

      prober =
        if fault == "kill",
          do:
            Task.async(fn ->
              Cluster.longest_write_outage(Cluster.port(c, survivor), probes, deadline)
            end)

      if fault == "pause" do
        Process.sleep(2 * c.ttl + 1_000)
        Cluster.signal(nodes[victim], "CONT")
      end

      acks = Enum.map(writers, &Task.await(&1, :infinity))
      recovery = if prober, do: Task.await(prober, :infinity)

      nodes =
        case fault do
          "kill" -> Map.put(nodes, victim, Cluster.start_node(c, victim, store))
          "pause" -> nodes
        end

      Cluster.create_all(Cluster.port(c, victim), probes)

      checks =
        for {acks, i} <- Enum.with_index(acks, 1),
            do: Verify.check(Cluster.port(c, survivor), Cluster.path(run, "w#{i}"), i, acks)

      for {_i, node} <- nodes, do: Cluster.signal(node, "TERM")

      for {_i, node} <- nodes do
        Stream.repeatedly(fn -> Cluster.alive?(node) && Process.sleep(100) end)
        |> Enum.find(&(&1 == false))
      end

      failures = for ch <- checks, ch.dupes != [] or ch.bad != [] or not ch.ordered, do: ch
      ok = failures == []
      acked = checks |> Enum.map(& &1.acked) |> Enum.sum()

      IO.puts(
        "run #{run} (#{fault} n#{victim}): #{if ok, do: "ok", else: "FAILED"}, " <>
          "#{acked} acknowledged appends checked" <>
          if(recovery, do: ", longest a shard could not be written: #{recovery} ms", else: "")
      )

      unless ok, do: IO.inspect(failures, label: "failures", limit: :infinity)
      {ok, fault, recovery}
    end
  after
    kill_all.()
  end

passed = Enum.count(results, &elem(&1, 0))
recoveries = for {_, _, r} <- results, r, do: r
IO.puts("#{passed} of #{config.runs} runs passed")

if recoveries != [],
  do: IO.puts("recovery: max #{Enum.max(recoveries)} ms over #{length(recoveries)} runs")

if passed == config.runs, do: File.rm_rf!(config.tmp), else: IO.puts("logs in #{config.tmp}")
if passed != config.runs, do: System.halt(1)
