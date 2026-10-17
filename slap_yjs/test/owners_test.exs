defmodule Slap.Yjs.OwnersTest do
  use ExUnit.Case, async: true

  alias Slap.Yjs.Owners

  setup do
    %{sub: spawn(fn -> :ok end), server: {:server, spawn(fn -> :ok end)}}
  end

  test "a client id belongs to the last owner that set it", %{sub: sub, server: server} do
    owners = %Owners{} |> Owners.changed(server, [1, 2], []) |> Owners.changed(sub, [1], [])

    assert {[2], remaining} = Owners.forget(owners, server)
    assert {[1], _} = Owners.forget(remaining, sub)
    assert Owners.local(owners, [1, 2, 3]) == [1, 3]
  end

  test "a removed id, or one set with no owner, has none", %{sub: sub, server: server} do
    owners =
      %Owners{}
      |> Owners.changed(server, [1, 2], [])
      |> Owners.changed(sub, [3], [1])
      |> Owners.changed(nil, [2], [])

    assert {[], _} = Owners.forget(owners, server)
    assert {[3], _} = Owners.forget(owners, sub)
    assert Owners.local(owners, [1, 2, 3]) == [1, 2, 3]
  end
end
