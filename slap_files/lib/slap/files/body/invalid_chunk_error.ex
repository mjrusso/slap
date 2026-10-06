defmodule Slap.Files.Body.InvalidChunkError do
  @moduledoc false
  defexception message: "body chunks must be binaries"
end
