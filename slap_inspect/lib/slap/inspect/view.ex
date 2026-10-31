defmodule Slap.Inspect.View do
  @moduledoc false
  use Breeze.View

  alias Slap.Inspect.{CLI, Help, Operation, Query, Requests}
  alias Slap.Inspect.View.{Dialogs, Layout, Navigation, Read}

  def mount(opts, term) do
    connection = Keyword.fetch!(opts, :connection)
    read = Keyword.get(opts, :read, &Query.execute(connection, &1))
    request = Keyword.fetch!(opts, :request)
    {:ok, worker} = Requests.start_link(client: self(), read: read)

    term =
      assign(term,
        connection: connection,
        request: request,
        worker: worker,
        page: nil,
        rows: [],
        selected: nil,
        detail: false,
        detail_cache: nil,
        mode: :browse,
        commands: [CLI.command(request)],
        display: :bytes,
        byte_history: [],
        observed_at: nil,
        stale: false,
        history: [],
        navigation: [],
        refresh: Keyword.get(opts, :refresh, 0),
        refresh_timer: nil
      )

    {:ok, term |> Read.start() |> Read.sync_refresh() |> Layout.prepare()}
  end

  defdelegate render(assigns), to: Layout
  def handle_info(message, term), do: prepare_reply(Read.handle_info(message, term))

  def handle_event(name, payload, term), do: prepare_reply(event(name, payload, term))

  defp prepare_reply({:noreply, term}),
    do: {:noreply, term |> Read.sync_refresh() |> Layout.prepare()}

  defp prepare_reply({:stop, term}), do: {:stop, term}

  defp event(
         :input,
         %{"key" => "u", "ctrlKey" => true},
         %{assigns: %{mode: {:editor, _, _}}} = term
       ),
       do: Dialogs.edit("\x15", term)

  defp event("pane", :switch_pane, %{assigns: %{rows: []}} = term), do: {:noreply, term}

  defp event("pane", :switch_pane, %{assigns: %{mode: :browse}} = term) do
    detail = not term.assigns.detail
    term = if detail and is_nil(term.assigns.selected), do: Navigation.select(term, 1), else: term

    {:noreply,
     term |> assign(detail: detail) |> focus(if(detail, do: "details", else: "records"))}
  end

  defp event("pane", _event, term), do: {:noreply, term}

  defp event(:input, %{"key" => key}, %{assigns: %{mode: {:editor, _, _}}} = term),
    do: Dialogs.edit(key, term)

  defp event(
         :input,
         %{"key" => key},
         %{assigns: %{mode: {:overlay, %{body: {:shards, shards, index}}}}} = term
       ),
       do: Dialogs.choose_shard(key, shards, index, term)

  defp event(:input, %{"key" => key}, %{assigns: %{mode: {:overlay, _}}} = term) do
    if key in ["Escape", "?", "e"],
      do: {:noreply, Dialogs.close_overlay(term)},
      else: {:noreply, term}
  end

  defp event(:input, %{"key" => "q"}, term), do: {:stop, term}

  defp event(:input, %{"key" => key}, %{assigns: %{mode: {:roots, _}}} = term),
    do: Dialogs.choose_root(key, term)

  defp event(
         :input,
         %{"key" => "y"},
         %{assigns: %{mode: {:confirm, _, _}}} = term
       ),
       do: Dialogs.confirm(term)

  defp event(
         :input,
         %{"key" => key},
         %{assigns: %{mode: {:confirm, _, _}}} = term
       )
       when key in ["n", "Escape"], do: Dialogs.cancel(term)

  defp event(:input, _payload, %{assigns: %{mode: {:confirm, _, _}}} = term),
    do: {:noreply, term}

  defp event(:input, %{"key" => key}, %{focused: "details"} = term)
       when key in ["ArrowDown", "ArrowUp"], do: {:noreply, term}

  defp event(:input, %{"key" => "ArrowDown"}, term),
    do: {:noreply, Navigation.select(term, 1)}

  defp event(:input, %{"key" => "ArrowUp"}, term),
    do: {:noreply, Navigation.select(term, -1)}

  defp event(:input, %{"key" => "Enter"}, %{assigns: %{rows: []}} = term),
    do: {:noreply, term}

  defp event(:input, %{"key" => "Enter"}, term) do
    term = if is_nil(term.assigns.selected), do: Navigation.select(term, 1), else: term
    {:noreply, term |> assign(detail: true) |> focus("details")}
  end

  defp event(:input, %{"key" => key}, term) when key in ["Escape", "b"],
    do: Navigation.back(term)

  defp event(:input, %{"key" => "/"}, term),
    do: {:noreply, Dialogs.editor(term, CLI.command(term.assigns.request))}

  defp event(:input, %{"key" => "s"}, term), do: Dialogs.open_shards(term)

  defp event(:input, %{"key" => "?"}, term),
    do: {:noreply, Dialogs.open_overlay(term, Help.text(term.assigns))}

  defp event(:input, %{"key" => "e"}, term), do: Dialogs.show_diagnostic(term)
  defp event(:input, %{"key" => "d"}, term), do: Navigation.display_mode(term)
  defp event(:input, %{"key" => "M"}, term), do: Navigation.previous_bytes(term)
  defp event(:input, %{"key" => "r"}, term), do: Read.refresh(term)
  defp event(:input, %{"key" => "n"}, term), do: Navigation.next_page(term)
  defp event(:input, %{"key" => "p"}, term), do: Navigation.previous_page(term)
  defp event(:input, %{"key" => "m"}, term), do: Navigation.more(term)
  defp event(:input, %{"key" => "f"}, term), do: Navigation.follow(term)
  defp event(:input, %{"key" => "g"}, term), do: Navigation.retention(term)
  defp event(:input, %{"key" => "t"}, term), do: {:noreply, Read.toggle_refresh(term)}
  defp event(:input, %{"key" => "c"}, term), do: Navigation.content(term)
  defp event(:input, %{"key" => "i"}, term), do: Dialogs.open_roots(term)

  defp event(:input, %{"key" => key}, term) when key in ["1", "2", "3", "4", "5", "6"],
    do: Navigation.switch(term, Enum.at(Operation.views(), String.to_integer(key) - 1))

  defp event(_event, _payload, term), do: {:noreply, term}
end
