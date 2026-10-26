defmodule Slap.SlateDB.LocalGCTest do
  use ExUnit.Case, async: true

  alias Slap.SlateDB
  alias Slap.SlateDB.{Admin, ObjectStore, Reader}

  @moduletag :capture_log
  @moduletag :tmp_dir

  test "repeated collection removes obsolete metadata with boundaries enabled", %{tmp_dir: dir} do
    store = {:local, dir}
    {:ok, objects} = ObjectStore.open("db", store: store)
    {:ok, admin} = Admin.open("db", store: store)

    for cycle <- 1..2 do
      {:ok, db} = SlateDB.open("db", store: store, settings: %{garbage_collector_options: nil})
      assert {:ok, _} = SlateDB.put(db, "key", "value-#{cycle}", await_durable: true)
      assert :ok = SlateDB.flush(db, type: :memtable)
      assert :ok = SlateDB.close(db)

      obsolete =
        for {prefix, suffix} <- [{"manifest/", ".manifest"}, {"compactions/", ".compactions"}] do
          {:ok, keys} = ObjectStore.list(objects, prefix)
          versions = keys |> Enum.filter(&String.ends_with?(&1, suffix)) |> Enum.sort()
          obsolete = Enum.drop(versions, -1)
          assert obsolete != []
          obsolete
        end

      assert :ok =
               Admin.run_gc(admin,
                 settings: %{
                   manifest_options: %{min_age: "0s"},
                   compactions_options: %{min_age: "0s"}
                 }
               )

      for key <- List.flatten(obsolete), do: assert({:ok, nil} == ObjectStore.get(objects, key))

      assert {:ok, reader} = Reader.open("db", store: store, mode: :latest)
      assert {:ok, value} = Reader.get(reader, "key")
      assert value == "value-#{cycle}"
      assert :ok = Reader.close(reader)
    end
  end
end
