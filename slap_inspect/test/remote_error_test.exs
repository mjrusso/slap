defmodule Slap.Inspect.RemoteErrorTest do
  use ExUnit.Case, async: true

  alias Slap.Inspect.Remote.Error

  test "diagnostics bound reasons and stack traces without retaining call arguments" do
    argument = String.duplicate("private argument", 1024)
    reason = {:deep_failure, List.duplicate(String.duplicate("🙂", 1000), 20)}

    stack =
      for line <- 1..32, do: {__MODULE__, :helper, [argument], file: ~c"source.ex", line: line}

    location = [file: ~c"source.ex", line: 0, error_info: %{argument: argument}]
    stack = [{fn -> :unused end, [argument], location} | stack]
    context = %{node: node(), endpoint: {__MODULE__, :endpoint, 1}}

    result = Error.result(context, :error, reason, stack)
    assert {:error, {:remote_call_failed, details}} = result
    assert details.reason =~ "deep_failure"
    assert byte_size(details.reason) <= 1024
    assert String.valid?(details.reason)
    assert Enum.count_until(details.stacktrace, 9) == 8
    assert Enum.all?(details.stacktrace, &(&1.arity == 1))
    assert :erlang.external_size(result) <= 8192
    refute inspect(details) =~ "private argument"
  end
end
