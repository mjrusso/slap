defmodule Slap.Test.StorageProxy do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks

  def start(prefix) do
    endpoint = URI.parse(System.fetch_env!("SLAP_TEST_S3_ENDPOINT"))
    assert endpoint.scheme == "http"
    parent = self()
    supervisor = start_supervised!({Task.Supervisor, []}, id: make_ref())

    gate =
      start_supervised!({Agent, fn -> %{hold: nil, waiting: [], reads: 0} end}, id: make_ref())

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)

    {:ok, acceptor} =
      Task.Supervisor.start_child(supervisor, fn ->
        accept(listener, endpoint, gate, parent, supervisor)
      end)

    # Shard shutdown can still flush to S3, so the proxy starts before the cluster.
    :ok = :gen_tcp.controlling_process(listener, acceptor)
    bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")
    prefix = "#{prefix}-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}"

    store =
      {:url, "s3://#{bucket}/#{prefix}",
       aws_endpoint: "http://127.0.0.1:#{port}",
       aws_allow_http: "true",
       aws_region: "us-east-1",
       aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
       aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")}

    %{gate: gate, store: store}
  end

  def hold(proxy, kind), do: Agent.update(proxy.gate, &%{&1 | hold: kind})
  def reads(proxy), do: Agent.get(proxy.gate, & &1.reads)

  def release(proxy, except \\ nil) do
    waiting =
      Agent.get_and_update(proxy.gate, fn state ->
        {state.waiting,
         %{state | hold: nil, waiting: Enum.filter(state.waiting, &(&1 == except))}}
      end)

    for pid <- waiting, pid != except, do: send(pid, :release)
  end

  defp accept(listener, endpoint, gate, parent, supervisor) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        {:ok, pid} =
          Task.Supervisor.start_child(supervisor, fn ->
            receive do: (:owned -> proxy(socket, endpoint, gate, parent))
          end)

        :ok = :gen_tcp.controlling_process(socket, pid)
        send(pid, :owned)
        accept(listener, endpoint, gate, parent, supervisor)

      {:error, :closed} ->
        :ok
    end
  end

  defp proxy(socket, endpoint, gate, parent) do
    {:ok, headers, body} = headers(socket, <<>>)

    if held?(gate, headers) do
      [request | _] = String.split(headers, "\r\n", parts: 2)
      send(parent, {:storage_blocked, self(), request})
      receive do: (:release -> :ok)
    end

    {:ok, upstream} =
      :gen_tcp.connect(
        String.to_charlist(endpoint.host),
        endpoint.port || 80,
        [:binary, active: false],
        5000
      )

    headers = Regex.replace(~r/\r\nconnection:[^\r\n]*/i, headers, "")
    :ok = :gen_tcp.send(upstream, [headers, "\r\nConnection: close\r\n\r\n", body])
    :ok = :inet.setopts(socket, active: true)
    :ok = :inet.setopts(upstream, active: true)
    relay(socket, upstream)
  end

  defp held?(gate, headers) do
    kind = kind(headers)
    pid = self()

    Agent.get_and_update(gate, fn state ->
      state = if kind == :sst, do: %{state | reads: state.reads + 1}, else: state

      if kind != nil and state.hold == kind,
        do: {true, %{state | waiting: [pid | state.waiting]}},
        else: {false, state}
    end)
  end

  # Manifest polling must continue while data reads or WAL uploads are blocked.
  defp kind("GET " <> rest), do: if(String.contains?(rest, ".sst HTTP/"), do: :sst)
  defp kind("PUT " <> rest), do: if(String.contains?(rest, "/wal/"), do: :wal)
  defp kind(_headers), do: nil

  defp headers(socket, acc) do
    case :binary.split(acc, "\r\n\r\n") do
      [headers, body] ->
        {:ok, headers, body}

      [_partial] ->
        with {:ok, bytes} <- :gen_tcp.recv(socket, 0, 5000), do: headers(socket, acc <> bytes)
    end
  end

  defp relay(socket, upstream) do
    receive do
      {:tcp, ^socket, bytes} ->
        :gen_tcp.send(upstream, bytes)
        relay(socket, upstream)

      {:tcp, ^upstream, bytes} ->
        :gen_tcp.send(socket, bytes)
        relay(socket, upstream)

      {:tcp_closed, _} ->
        :gen_tcp.close(socket)
        :gen_tcp.close(upstream)
    end
  end
end
