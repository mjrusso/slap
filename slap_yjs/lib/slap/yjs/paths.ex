defmodule Slap.Yjs.Paths do
  @moduledoc false

  # Inspection must parse document paths without calling `Slap.Yjs.Store`.

  def base(prefix, {service, name}), do: "#{prefix}/yjs/#{service}/docs/#{name}"

  @doc "Returns `{:ok, {service, name}}` for a document's base path, or `:error`."
  def parse_base(base) do
    with [_base, service, name] <- Regex.run(~r{^/.*/yjs/([^/]+)/docs/([^/]+)$}, base),
         true <- valid_doc?({service, name}),
         do: {:ok, {service, name}},
         else: (_ -> :error)
  end

  @doc "Checks that `doc` is `{service, name}` with nonempty path segments that do not start with a dot."
  def valid_doc?({service, name}), do: valid_segment?(service) and valid_segment?(name)
  def valid_doc?(_doc), do: false

  defp valid_segment?(segment),
    do:
      is_binary(segment) and segment != "" and not String.starts_with?(segment, ".") and
        not String.contains?(segment, "/")
end
