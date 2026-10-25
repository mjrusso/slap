defmodule Slap.Streams.LargeBodyTest do
  use ExUnit.Case, async: true

  alias Slap.Streams.Protocol

  test "a one-GiB binary remains one nonempty message" do
    bytes = :binary.copy("a", 1024 * 1024 * 1024)
    assert {:ok, [^bytes]} = Protocol.split("application/octet-stream", bytes, true)
  end
end
