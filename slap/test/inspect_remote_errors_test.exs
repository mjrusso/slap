defmodule Slap.InspectRemoteErrorsTest do
  # Distribution setup is shared with the other peer tests on the test node.
  use ExUnit.Case, async: false

  import Slap.Test.InspectPeers

  alias Slap.Inspect.Remote
  alias Slap.Streams.Introspect

  setup do
    distribute()
    {_peer, owner} = peer("inspect-errors")

    eval(owner, """
    Code.compiler_options(ignore_module_conflict: true)

    defmodule Slap.Cluster do
      def assignments(_cluster), do: %{0 => {:local, self()}}
    end

    defmodule Slap.Streams.Introspect do
      def shard_for(_cluster, _version, path),
        do: apply(Slap.Test.MissingInspectionHelper, :shard, [path])

      def list(_cluster, _shard, _version, budget, opts) do
        payload = opts[:payload]

        case opts[:failure] do
          :not_owner ->
            send(opts[:report], {:budget, budget})
            Process.sleep(200)
            {:error, :not_owner}

          :error -> raise "inspection_failed: " <> payload
          :exit -> exit({:inspection_failed, payload})
          :throw -> throw({:inspection_failed, payload})
          :undef -> apply(Slap.Test.MissingInspectionHelper, :read, [payload])
          :raised_undef -> raise UndefinedFunctionError, module: __MODULE__, function: :list, arity: 5
          :bare_undef -> :erlang.error(:undef)
          :exit_noconnection -> exit(:noconnection)
          :fake_disconnect -> :erlang.error({:erpc, :noconnection})
          :signal -> Process.exit(self(), :kill)
        end
      end
    end
    """)

    %{owner: owner}
  end

  test "endpoint errors, exits and throws retain bounded diagnostics", %{owner: owner} do
    payload = String.duplicate("x", 1024 * 1024)

    for kind <- [:error, :exit, :throw] do
      assert {:error,
              {:remote_call_failed,
               %{
                 node: ^owner,
                 endpoint: {Introspect, :list, 5},
                 kind: ^kind,
                 reason: reason,
                 stacktrace: _stack
               } = diagnostic}} = read(owner, failure: kind, payload: payload)

      assert reason =~ "inspection_failed"
      assert byte_size(reason) <= 1024
      assert :erlang.external_size(diagnostic) <= 8192
    end
  end

  test "undefined calls inside a supported endpoint are failures, not unsupported APIs", %{
    owner: owner
  } do
    assert {:error, {:remote_call_failed, %{kind: :error, stacktrace: [frame | _]}}} =
             read(owner, failure: :undef, payload: "unused")

    assert %{module: Slap.Test.MissingInspectionHelper, function: :read, arity: 1} = frame

    for failure <- [:bare_undef, :raised_undef] do
      assert {:error, {:remote_call_failed, %{kind: :error}}} = read(owner, failure: failure)
    end

    assert {:error, :owner_does_not_support_inspection} =
             Remote.read(
               connection(owner),
               %{shard: 0, opts: []},
               {Introspect, :stream},
               deadline()
             )
  end

  test "endpoint exits and signals do not imply a lost node connection", %{owner: owner} do
    for {failure, kind} <- [exit_noconnection: :exit, signal: :exit, fake_disconnect: :error] do
      assert {:error, {:remote_call_failed, %{kind: ^kind}}} = read(owner, failure: failure)
      assert owner in Node.list(:connected)
    end
  end

  test "ownership retries get the remaining time and stop at the deadline", %{owner: owner} do
    deadline = System.monotonic_time(:millisecond) + 300
    request = %{shard: 0, opts: [failure: :not_owner, report: self()]}

    assert {:error, :deadline} =
             Remote.read(connection(owner), request, {Introspect, :list}, deadline)

    assert_receive {:budget, first}
    assert_receive {:budget, retry}
    assert first <= 300
    assert retry <= first - 200
    refute_receive {:budget, _}
  end

  test "metadata calls retain their endpoint and nested undefined function", %{owner: owner} do
    assert {:error,
            {:remote_call_failed,
             %{endpoint: {Introspect, :shard_for, 3}, stacktrace: [frame | _]}}} =
             Remote.call(owner, Introspect, :shard_for, [Slap.Streams.Cluster, 1, "/p"])

    assert %{module: Slap.Test.MissingInspectionHelper, function: :shard, arity: 1} = frame
  end

  defp read(owner, opts),
    do: Remote.read(connection(owner), %{shard: 0, opts: opts}, {Introspect, :list}, deadline())

  defp connection(owner), do: %{entry: owner, cluster: Slap.Streams.Cluster}
  defp deadline, do: System.monotonic_time(:millisecond) + 5000
end
