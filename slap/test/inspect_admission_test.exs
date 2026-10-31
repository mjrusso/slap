defmodule Slap.InspectAdmissionTest do
  # Admission is shared by every package on the node, including the named Files instance.
  use ExUnit.Case, async: false

  alias Slap.Cluster.Inspection
  alias Slap.Files.Introspect
  alias Slap.KV
  alias Slap.Yjs.Frame

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)
    on_exit(fn -> :gen_tcp.close(listener) end)
    supervisor = start_supervised!(Task.Supervisor)
    parent = self()
    start_supervised!({KV.Cluster, store: :memory, shards: 1})
    assert :ok = KV.Cluster.await_ready(5000)

    start_supervised!(
      {Slap.Files,
       store: store(port), sweep_interval_ms: 3_600_000, reconcile_interval_ms: 3_600_000}
    )

    %{listener: listener, supervisor: supervisor, parent: parent}
  end

  test "an abandoned client cannot release an owner blocked in native object storage", context do
    {:ok, _server} =
      Task.Supervisor.start_child(context.supervisor, fn ->
        storage(context.listener, context.parent, :headers)
      end)

    opts = [object_key: "objects/ns/ZGVmYXVsdA/0/example", value_range: {0, 4}]

    client =
      Task.async(fn ->
        request = :erpc.send_request(node(), Introspect, :object, [KV.Cluster, 0, 1, 50, opts])
        :erpc.receive_response(request, :infinity)
      end)

    assert_receive {:storage_blocked, storage}, 5000
    owner = Process.whereis(Inspection)
    assert is_pid(owner)
    owner_ref = Process.monitor(owner)
    Process.send_after(self(), :past_budget, 100)
    assert_receive :past_budget, 5000
    assert {:error, :busy} = KV.Introspect.scan(KV.Cluster, 0, 1, 1000, [])

    Task.shutdown(client, :brutal_kill)
    assert Process.alive?(owner)
    assert {:error, :busy} = KV.Introspect.scan(KV.Cluster, 0, 1, 1000, [])
    assert {:error, :busy} = Slap.Streams.Introspect.scan(Slap.Streams.Cluster, 0, 1, 1000, [])
    send(storage, :release)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, _reason}, 5000
    assert {:ok, _page} = KV.Introspect.scan(KV.Cluster, 0, 1, 1000, [])
  end

  test "an object preview preserves partial bytes and reports an expired deadline", context do
    {:ok, _server} =
      Task.Supervisor.start_child(context.supervisor, fn ->
        storage(context.listener, context.parent, :body)
      end)

    opts = [object_key: "objects/ns/ZGVmYXVsdA/0/example", value_range: {0, 4}]

    reader =
      Task.async(fn ->
        receive do: (:begin -> Introspect.object(KV.Cluster, 0, 1, 1000, opts))
      end)

    :erlang.trace_pattern({Slap.SlateDB.Native, :objstore_download_next, 2}, true, [:local])

    on_exit(fn ->
      :erlang.trace_pattern({Slap.SlateDB.Native, :objstore_download_next, 2}, false, [:local])
    end)

    :erlang.trace(reader.pid, true, [:call])
    send(reader.pid, :begin)
    assert_receive {:storage_blocked, storage}, 5000
    assert_receive {:trace, _, :call, {Slap.SlateDB.Native, :objstore_download_next, _}}, 5000
    Process.send_after(storage, :release, 1100)

    assert {:ok, %{stopped: :deadline, object: %{value: "da", value_size: 4, offset: 0}}} =
             Task.await(reader, 5000)

    send(storage, :finish)
    assert {:ok, _} = KV.Introspect.scan(KV.Cluster, 0, 1, 1000, [])
  end

  test "message interpretation retains admission through annotation and snapshot selection",
       context do
    start_supervised!({Slap.Streams.Cluster, store: :memory, shards: 1})
    assert :ok = Slap.Streams.Cluster.await_ready(5000)
    parent = self()
    annotation = {Slap.Streams.Introspect.Messages, :annotate, 2}
    selection = {Slap.SnapshotLog.Introspect.Capture, :index_selection, 2}
    release = {:erlang, :unregister, 1}

    for function <- [annotation, selection] do
      Code.ensure_loaded!(elem(function, 0))
      :erlang.trace_pattern(function, [{:_, [], [{:return_trace}]}], [])
    end

    :erlang.trace_pattern(release, [{[Inspection], [], []}], [])

    on_exit(fn ->
      for function <- [annotation, selection, release],
          do: :erlang.trace_pattern(function, false, [])
    end)

    for {module, body, opts, stages} <- [
          {Slap.Yjs.Introspect, Frame.frame("update"), [], [annotation]},
          {Slap.SnapshotLog.Introspect,
           ~s({"snapshotOffset":"0000000000000000_0000000000000012","createdAt":5}),
           [component: :index], [annotation, selection]}
        ] do
      path = "/admission/#{module}"
      assert {:ok, _, _} = Slap.Streams.create(path, body: body)
      {:ok, %{sid: sid}} = Slap.Streams.metadata_fenced(path)

      reader =
        Task.Supervisor.async(context.supervisor, fn ->
          receive do
            :begin -> module.messages(Slap.Streams.Cluster, 0, 1, 5000, [sid: sid] ++ opts)
          end
        end)

      :erlang.trace(reader.pid, true, [:call, :arity, {:tracer, parent}])
      send(reader.pid, :begin)
      assert {:ok, _page} = Task.await(reader, 5000)

      for stage <- stages do
        assert_receive {:trace, pid, :call, function}
        assert pid == reader.pid
        assert function == stage
        assert_receive {:trace, ^pid, :return_from, ^stage, _result}
      end

      assert_receive {:trace, _, :call, ^release}
      assert Process.whereis(Inspection) == nil
    end
  end

  defp storage(listener, parent, mode) do
    {:ok, socket} = :gen_tcp.accept(listener)
    {:ok, request} = :gen_tcp.recv(socket, 0, 5000)
    assert request =~ "GET "

    headers = [
      "HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\n",
      "Content-Range: bytes 0-3/4\r\nETag: \"test\"\r\n",
      "Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n"
    ]

    if mode == :body, do: :ok = :gen_tcp.send(socket, headers)
    send(parent, {:storage_blocked, self()})
    receive do: (:release -> :ok)

    case mode do
      :headers ->
        :ok = :gen_tcp.send(socket, [headers, "data"])

      :body ->
        :ok = :gen_tcp.send(socket, "da")
        receive do: (:finish -> :ok)
        :gen_tcp.send(socket, "ta")
    end

    :gen_tcp.close(socket)
  end

  defp store(port) do
    {:url, "s3://inspect-test",
     aws_endpoint: "http://127.0.0.1:#{port}",
     aws_allow_http: "true",
     aws_region: "us-east-1",
     aws_access_key_id: "test",
     aws_secret_access_key: "test"}
  end
end
