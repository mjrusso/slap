defmodule Slap.SnapshotLog.IntrospectTest do
  use ExUnit.Case, async: true
  alias Slap.SnapshotLog.Introspect

  test "recognizes control and snapshot naming without assuming every prefix is a log" do
    assert {:ok, %{base: "/log", component: :control}} = Introspect.component("/log/.control")

    assert {:ok, %{offset: 12, component: :snapshot}} =
             Introspect.component("/log/.snapshots/0000000000000000_0000000000000012_snapshot")

    assert {:error, _} = Introspect.component("/log/.snapshots/invalid_snapshot")

    assert {:ok, %{state: :forking}} =
             Introspect.decode(:control, ~s({"kind":"fork","status":"pending"}))

    assert {:error, :malformed_control} = Introspect.decode(:control, ~s({"kind":"unknown"}))

    assert {:ok, %{offset: 12}} =
             Introspect.decode(
               :index,
               ~s({"snapshotOffset":"0000000000000000_0000000000000012","createdAt":5})
             )

    assert {:error, :malformed_index} = Introspect.decode(:index, "bad")
  end
end
