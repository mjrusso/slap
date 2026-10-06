# kill -9 under load. Each run starts `mix slap.server` as its own OS
# process, has writers append to their own streams, kills the server with
# SIGKILL at a random moment, restarts it on the same store, lets the
# writers carry on, then reads every stream back and checks that:
#
#   * every acknowledged append is there, exactly once, at the offset its
#     acknowledgement gave (so no offset was reused after the crash);
#   * no message appears twice, and each writer's messages are in order;
#   * a producer's retry of an append that was in flight when the server
#     died is applied once (as the original or as a duplicate).
#
#     mix run scripts/crash_test.exs [--runs 20] [--writers 16]
#         [--store local | s3:s3://bucket/prefix] [--port 4540]
#         [--min-ms 300] [--max-ms 3000]
#
# Half the writers use idempotent producers. For s3, the AWS_* variables
# configure the endpoint and credentials; each run uses its own prefix.

Code.require_file("../bench/support/raw_http.exs", __DIR__)
alias Slap.Bench.RawHTTP

{opts, _} =
  OptionParser.parse!(System.argv(),
    strict: [
      runs: :integer,
      writers: :integer,
      store: :string,
      port: :integer,
      min_ms: :integer,
      max_ms: :integer
    ]
  )

runs = Keyword.get(opts, :runs, 20)
writers = Keyword.get(opts, :writers, 16)
port = Keyword.get(opts, :port, 4540)
{min_ms, max_ms} = {Keyword.get(opts, :min_ms, 300), Keyword.get(opts, :max_ms, 3000)}
store_opt = Keyword.get(opts, :store, "local")
tmp = Path.join(System.tmp_dir!(), "slap-crash-#{System.os_time(:millisecond)}")
mix = System.find_executable("mix")
project = Path.expand("..", __DIR__)

defmodule Crash do
  def start_server(mix, project, store, port, pid_file) do
    File.rm(pid_file)

    server =
      Port.open({:spawn_executable, mix}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        cd: project,
        args:
          ~w(slap.server --streams --port #{port} --store #{store} --streams-shards 4 --pid-file #{pid_file})
      ])

    wait_ready(port, 600)
    {server, pid_file |> File.read!() |> String.trim()}
  end

  defp wait_ready(_port, 0), do: raise("the server did not start")

  defp wait_ready(port, tries) do
    case RawHTTP.request_once(port, "HEAD", "/v1/stream/ready") do
      {:ok, _, _, _} ->
        :ok

      _ ->
        Process.sleep(100)
        wait_ready(port, tries - 1)
    end
  end

  def kill(server, os_pid) do
    {_, 0} = System.cmd("kill", ["-9", os_pid])

    receive do
      {^server, {:exit_status, _}} -> :ok
    after
      30_000 -> raise "the server did not exit"
    end

    flush(server)
  end

  defp flush(server) do
    receive do
      {^server, _} -> flush(server)
    after
      0 -> :ok
    end
  end

  def path(i), do: "/v1/stream/crash/w#{i}"

  # Appends until told to stop or the connection drops. Returns
  # {acks, in_doubt, next_n}: acks are {n, next_offset | :duplicate}.
  def write(port, i, producer?, n, stop) do
    {:ok, s} = RawHTTP.connect(port)
    loop(s, i, producer?, n, stop, [])
  end

  defp loop(s, i, producer?, n, stop, acks) do
    if :atomics.get(stop, 1) == 1 do
      RawHTTP.close(s)
      {Enum.reverse(acks), nil, n}
    else
      case append(s, i, producer?, n) do
        {:ok, ack} -> loop(s, i, producer?, n + 1, stop, [{n, ack} | acks])
        :retry -> Process.sleep(10) && loop(s, i, producer?, n, stop, acks)
        :dropped -> {Enum.reverse(acks), n, n + 1}
      end
    end
  end

  def append(s, i, producer?, n) do
    headers =
      [{"content-type", "application/json"}] ++
        if producer?,
          do: [{"producer-id", "w#{i}"}, {"producer-epoch", "0"}, {"producer-seq", "#{n}"}],
          else: []

    case RawHTTP.request(s, "POST", path(i), headers, ~s("w#{i}-#{n}")) do
      {:ok, status, h, _} when status in [200, 204] ->
        # A duplicate's Stream-Next-Offset is the tail, not the message's end.
        if producer? and status == 204,
          do: {:ok, :duplicate},
          else: {:ok, offset(RawHTTP.header(h, "stream-next-offset"))}

      {:ok, 503, _, _} ->
        :retry

      {:ok, status, _, body} ->
        raise "append w#{i}-#{n}: #{status} #{body}"

      {:error, _} ->
        :dropped
    end
  end

  defp offset(wire) do
    {:ok, o} = Slap.Streams.Offset.parse(wire)
    o
  end

  def read_all(port, i) do
    {:ok, s} = RawHTTP.connect(port)
    messages = read_from(s, i, "-1", 0, [])
    RawHTTP.close(s)
    messages
  end

  defp read_from(s, i, wire, at, acc) do
    {:ok, 200, h, body} = RawHTTP.request(s, "GET", "#{path(i)}?offset=#{wire}")
    {:ok, values} = JSON.decode(body)

    {placed, at} =
      Enum.map_reduce(values, at, fn v, at ->
        size = byte_size(JSON.encode!(v))
        {{v, at, at + 4 + size}, at + 4 + size}
      end)

    acc = acc ++ placed

    if RawHTTP.header(h, "stream-up-to-date") == "true",
      do: acc,
      else: read_from(s, i, RawHTTP.header(h, "stream-next-offset"), at, acc)
  end
end

results =
  for run <- 1..runs do
    store =
      case store_opt do
        "local" -> "local:#{tmp}/run-#{run}"
        "s3:" <> url -> "s3:#{url}/run-#{run}-#{System.os_time(:millisecond)}"
      end

    pid_file = "#{tmp}/server.pid"
    File.mkdir_p!(tmp)
    {server, os_pid} = Crash.start_server(mix, project, store, port, pid_file)

    {:ok, s} = RawHTTP.connect(port)

    for i <- 1..writers do
      {:ok, 201, _, _} =
        RawHTTP.request(s, "PUT", Crash.path(i), [{"content-type", "application/json"}])
    end

    RawHTTP.close(s)

    stop = :atomics.new(1, [])

    tasks =
      for i <- 1..writers, do: Task.async(fn -> Crash.write(port, i, rem(i, 2) == 0, 0, stop) end)

    Process.sleep(min_ms + :rand.uniform(max_ms - min_ms))
    Crash.kill(server, os_pid)
    first = Enum.map(tasks, &Task.await(&1, 60_000))

    {server, os_pid} = Crash.start_server(mix, project, store, port, pid_file)

    second =
      for {{acks, in_doubt, next}, i} <- Enum.with_index(first, 1) do
        producer? = rem(i, 2) == 0
        {:ok, s} = RawHTTP.connect(port)

        retried =
          if producer? and in_doubt do
            {:ok, ack} = Crash.append(s, i, true, in_doubt)
            [{in_doubt, ack}]
          else
            []
          end

        more =
          for n <- next..(next + 9) do
            {:ok, ack} = Crash.append(s, i, producer?, n)
            {n, ack}
          end

        RawHTTP.close(s)
        {acks ++ retried ++ more, in_doubt}
      end

    checks =
      for {{acks, in_doubt}, i} <- Enum.with_index(second, 1) do
        messages = Crash.read_all(port, i)
        by_body = Enum.group_by(messages, fn {v, _, _} -> v end)
        dupes = for {v, list} <- by_body, length(list) > 1, do: v

        missing_or_moved =
          for {n, ack} <- acks,
              body = "w#{i}-#{n}",
              found = Map.get(by_body, body, []),
              not match?([_], found) or (ack != :duplicate and elem(hd(found), 2) != ack),
              do: {body, ack, found}

        ns =
          for {v, _, _} <- messages,
              do: v |> String.split("-") |> List.last() |> String.to_integer()

        ordered = ns == Enum.sort(ns)
        # Only for plain writers: a producer retried it.
        in_doubt_kept =
          rem(i, 2) == 1 and in_doubt != nil and Map.has_key?(by_body, "w#{i}-#{in_doubt}")

        %{
          acked: length(acks),
          dupes: dupes,
          bad: missing_or_moved,
          ordered: ordered,
          in_doubt: in_doubt != nil,
          in_doubt_kept: in_doubt_kept
        }
      end

    Crash.kill(server, os_pid)

    acked = Enum.sum(Enum.map(checks, & &1.acked))
    in_doubt = Enum.count(checks, & &1.in_doubt)
    kept = Enum.count(checks, & &1.in_doubt_kept)
    failures = for c <- checks, c.dupes != [] or c.bad != [] or not c.ordered, do: c
    ok = failures == []

    IO.puts(
      "run #{run}: #{if ok, do: "ok", else: "FAILED"}, #{acked} acknowledged appends checked, " <>
        "#{in_doubt} in flight at the kill (#{kept} of the plain writers' stored anyway)"
    )

    unless ok, do: IO.inspect(failures, label: "failures", limit: :infinity)
    ok
  end

File.rm_rf!(tmp)
passed = Enum.count(results, & &1)
IO.puts("#{passed} of #{runs} runs passed")
if passed != runs, do: System.halt(1)
