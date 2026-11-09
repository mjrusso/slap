defmodule Slap.Inspect.View.Dialogs do
  @moduledoc false
  import Breeze.View, only: [focus: 2]
  import Breeze.Component, only: [assign: 2]

  alias Slap.Inspect.{CLI, Command, Editor, Feedback, Request}
  alias Slap.Inspect.View.Read

  @root_types [nil, :text, :array, :map, :xml_fragment]

  def open_shards(term) do
    id = {:shards, make_ref()}
    Read.submit(term, id, Request.shards())

    {:noreply, assign(term, id: id, operation: :reading, status: "Loading available shards")}
  end

  def choose_shard("Escape", _shards, _index, term),
    do: {:noreply, term |> assign(status: "Shard selection cancelled") |> close_overlay()}

  def choose_shard("Enter", shards, index, term) do
    request = %{
      term.assigns.request
      | shard: Enum.at(shards, index),
        opts: Keyword.drop(term.assigns.request.opts, [:cursor, :cursor_exclusive])
    }

    {:noreply, term |> assign(mode: :browse, request: request, history: []) |> Read.page()}
  end

  def choose_shard(key, shards, index, term) when key in ["ArrowUp", "ArrowDown"] do
    step = if key == "ArrowUp", do: -1, else: 1
    index = max(0, min(index + step, length(shards) - 1))
    {:overlay, overlay} = term.assigns.mode
    {:noreply, assign(term, mode: {:overlay, %{overlay | body: {:shards, shards, index}}})}
  end

  def choose_shard(_key, _shards, _index, term), do: {:noreply, term}

  def show_diagnostic(term) do
    text =
      "Diagnostics\n\n" <>
        inspect(term.assigns.diagnostic || "No error recorded",
          pretty: true,
          width: 70,
          limit: :infinity,
          printable_limit: :infinity
        )

    {:noreply, open_overlay(term, text)}
  end

  def open_overlay(term, body) do
    term
    |> assign(mode: {:overlay, %{body: body, focus: term.focused, return: :browse}})
    |> focus("overlay")
  end

  def close_overlay(term) do
    {:overlay, overlay} = term.assigns.mode
    term |> assign(mode: overlay.return) |> focus(overlay.focus)
  end

  def open_roots(
        %{
          assigns: %{
            operation: :idle,
            request: %{view: :yjs_content} = request,
            page: %{content: {:json, json}}
          }
        } = term
      ) do
    choices = Map.new(request.opts[:roots] || [])
    roots = for root <- JSON.decode!(json)["roots"], do: {root["name"], choices[root["name"]]}

    case roots do
      [] ->
        {:noreply, assign(term, status: "This document has no roots")}

      _roots ->
        {:noreply,
         term
         |> assign(mode: {:roots, %{choices: roots, index: 0}})
         |> focus("root-chooser")}
    end
  end

  def open_roots(%{assigns: %{operation: operation}} = term) when operation != :idle,
    do: {:noreply, term}

  def open_roots(term),
    do: {:noreply, assign(term, status: "Load Yjs content to choose its roots")}

  def choose_root("Escape", term), do: {:noreply, close_roots(term)}

  def choose_root("Enter", term) do
    {:roots, chooser} = term.assigns.mode
    roots = Enum.reject(chooser.choices, fn {_name, type} -> is_nil(type) end)
    request = term.assigns.request
    request = %{request | opts: Keyword.put(request.opts, :roots, roots)}
    {:noreply, term |> close_roots() |> Read.start(request)}
  end

  def choose_root(key, term) when key in ["ArrowUp", "ArrowDown"] do
    {:roots, chooser} = term.assigns.mode
    delta = if key == "ArrowUp", do: -1, else: 1
    index = max(0, min(chooser.index + delta, length(chooser.choices) - 1))
    {:noreply, assign(term, mode: {:roots, %{chooser | index: index}})}
  end

  def choose_root(key, term) when key in ["ArrowLeft", "ArrowRight"] do
    {:roots, chooser} = term.assigns.mode
    delta = if key == "ArrowLeft", do: -1, else: 1

    choices =
      List.update_at(chooser.choices, chooser.index, fn {name, type} ->
        index = Enum.find_index(@root_types, &(&1 == type))
        {name, Enum.at(@root_types, Integer.mod(index + delta, length(@root_types)))}
      end)

    {:noreply, assign(term, mode: {:roots, %{chooser | choices: choices}})}
  end

  def choose_root(_key, term), do: {:noreply, term}

  defp close_roots(term),
    do:
      term
      |> assign(mode: :browse)
      |> focus(if(term.assigns.detail, do: "details", else: "records"))

  def edit("Escape", term) do
    {:editor, _editor, return_mode} = term.assigns.mode
    {:noreply, assign(term, mode: return_mode)}
  end

  def edit("Enter", term) do
    {:editor, editor, _return_mode} = term.assigns.mode

    with {:ok, tokens} <- Command.split(editor.text),
         {:ok, request} <- CLI.parse_query(tokens) do
      commands =
        [
          editor.text
          | Enum.reject(term.assigns.commands, &(&1 == editor.text))
        ]
        |> Enum.take(50)

      {:noreply,
       term
       |> assign(request: request, mode: :browse, commands: commands, history: [])
       |> Read.page()}
    else
      {:error, reason} ->
        {:noreply,
         assign(term, status: "Command error: " <> Feedback.error(reason), diagnostic: reason)}
    end
  end

  def edit(key, term) do
    {:editor, editor, return_mode} = term.assigns.mode
    {:noreply, assign(term, mode: {:editor, Editor.input(editor, key), return_mode})}
  end

  def editor(term, command) do
    history = Enum.reject(term.assigns.commands, &(&1 == command))
    assign(term, mode: {:editor, Editor.new(command, history), term.assigns.mode})
  end

  def confirm(%{assigns: %{mode: {:confirm, request, _capture}}} = term) do
    id = make_ref()
    Read.submit(term, id, request)

    {:noreply,
     assign(term,
       id: id,
       request: request,
       mode: :browse,
       operation: :reading,
       status: "Reconstructing"
     )}
  end

  def cancel(term),
    do:
      {:noreply,
       assign(term,
         id: make_ref(),
         mode: :browse,
         operation: :idle,
         status: "Reconstruction cancelled"
       )}
end
