defmodule Slap.Test.InspectPeers do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks

  def distribute do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])
      {:ok, _} = Node.start(:"inspect-test-#{System.unique_integer([:positive])}@127.0.0.1")
    end
  end

  def peer(name, extra \\ []) do
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])

    options = %{
      name: peer_name(name),
      host: ~c"127.0.0.1",
      longnames: true,
      connection: :standard_io,
      args: [
        ~c"+S",
        ~c"2",
        ~c"-setcookie",
        Atom.to_charlist(Node.get_cookie()) | extra ++ paths
      ]
    }

    spec = %{id: make_ref(), start: {:peer, :start_link, [options]}, restart: :temporary}
    pid = start_supervised!(spec)
    node = :peer.call(pid, Node, :self, [], 5000)
    assert {:ok, _} = :peer.call(pid, :application, :ensure_all_started, [:elixir], 10_000)
    {pid, node}
  end

  def eval(node, code, bindings \\ []),
    do: call(node, Code, :eval_string, [code, bindings], 30_000)

  defp peer_name(name) when is_atom(name),
    do: name |> Atom.to_string() |> String.split("@") |> hd() |> String.to_atom()

  defp peer_name(name), do: :"#{name}-#{System.unique_integer([:positive])}"

  defp call(target, module, function, args, timeout) when is_pid(target),
    do: :peer.call(target, module, function, args, timeout)

  defp call(target, module, function, args, timeout),
    do: :erpc.call(target, module, function, args, timeout)
end
