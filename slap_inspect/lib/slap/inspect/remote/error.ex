defmodule Slap.Inspect.Remote.Error do
  @moduledoc false

  @max_bytes 8192
  @max_frames 8

  def result(context, kind, reason, stack) do
    details = Map.merge(context, %{kind: kind, reason: summary(reason, 1024), stacktrace: []})

    details =
      stack
      |> Enum.take(@max_frames)
      |> Enum.reduce_while(details, fn frame, details ->
        candidate = %{details | stacktrace: [frame(frame) | details.stacktrace]}

        if :erlang.external_size({:error, {:remote_call_failed, candidate}}) <= @max_bytes,
          do: {:cont, candidate},
          else: {:halt, details}
      end)

    {:error, {:remote_call_failed, %{details | stacktrace: Enum.reverse(details.stacktrace)}}}
  end

  defp frame({module, function, args, location})
       when is_atom(module) and is_atom(function) and is_list(location) and
              (is_list(args) or is_integer(args)) do
    %{
      module: module,
      function: function,
      arity: if(is_list(args), do: length(args), else: args),
      location: summary(Keyword.take(location, [:file, :line]), 512)
    }
  end

  defp frame({function, args, location}) when is_function(function) do
    {:module, module} = :erlang.fun_info(function, :module)
    {:name, name} = :erlang.fun_info(function, :name)
    frame({module, name, args, location})
  end

  defp frame(_frame), do: %{diagnostic: :unrecognized_stack_frame}

  defp summary(term, bytes) do
    term
    |> inspect(structs: false, limit: 10, printable_limit: 256)
    |> clip(bytes)
  end

  defp clip(text, bytes) when byte_size(text) <= bytes, do: text

  defp clip(text, bytes) do
    case :unicode.characters_to_binary(binary_part(text, 0, bytes - 3)) do
      {_status, valid, _rest} -> valid <> "..."
      valid -> valid <> "..."
    end
  end
end
