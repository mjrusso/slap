defmodule Slap.Inspect.View.Layout do
  @moduledoc false
  use Breeze.Component

  alias BackBreeze.Utils

  alias Slap.Inspect.{
    Bytes,
    CLI,
    Details,
    Editor,
    Help,
    Labels,
    Operation,
    Prefix,
    Rows
  }

  alias Slap.Inspect.View.Activity

  # The terminal width from which navigation, records and details share the screen.
  @two_panes 110
  @navigation_width 17..32

  def prepare(term) do
    assigns = term.assigns
    width = term.terminal.size.width
    panes = presentation(assigns.mode)

    visible =
      is_nil(panes.overlay) and is_nil(panes.root_chooser) and
        (assigns.detail or width >= @two_panes)

    if visible,
      do: cache_details(term, pane_width(width, navigation_width(assigns.menu))),
      else: term
  end

  defp cache_details(%{assigns: assigns} = term, width) do
    row = Rows.selected(assigns)
    empty = assigns.rows == [] and not is_nil(assigns.page)
    key = {row, empty, assigns.preview, assigns.display, width}

    case assigns.detail_cache do
      %{key: ^key} ->
        term

      _ ->
        {mode, text} =
          if empty,
            do: {assigns.display, "This page has no records."},
            else: Details.render(row, assigns.preview, assigns.display, width)

        assign(term, detail_cache: %{key: key, text: text, width: text_width(text), mode: mode})
    end
  end

  def render(assigns) do
    assigns = assign(assigns, presentation(assigns.mode))
    row = Rows.selected(assigns)
    navigation_width = navigation_width(assigns.menu)
    width = pane_width(assigns.breeze.terminal.width, navigation_width)
    details = assigns.detail_cache || %{text: "", width: 0, mode: assigns.display}
    height = body_height(assigns)
    overlay = overlay_text(assigns.overlay, height)

    assigns =
      assign(assigns,
        detail_text: details.text,
        detail_width: details.width,
        detail_mode: details.mode,
        record_width: width + 2,
        wide: assigns.breeze.terminal.width >= @two_panes,
        overlay_text: overlay,
        overlay_width: text_width(overlay),
        detail_key:
          {assigns.request, assigns.selected, assigns.display,
           assigns.preview && assigns.preview[:offset]},
        command:
          Bytes.head(
            "Query: " <> Bytes.display(CLI.command(assigns.request)),
            assigns.breeze.terminal.width
          ),
        title: title_line(assigns),
        page_label: page_label(assigns),
        help_lines: Help.lines(assigns, row, assigns.breeze.terminal.width >= @two_panes),
        body_height: height,
        navigation: navigation(assigns.menu, {assigns.connection.cluster, assigns.request.view}),
        navigation_width: navigation_width
      )

    ~H"""
    <box class="block w-screen h-screen">
      <box class="h-1 w-full font-bold overflow-hidden">{@title}</box>
      <box class="h-1 w-full overflow-hidden">{@command}</box>
      <box class={"h-1 w-full overflow-hidden " <> Activity.class(@activity)}>
        {Activity.line(@activity, status(@status, @page, @preview))}
      </box>
      <box :if={not is_nil(@editor)} class="h-3 w-full border overflow-hidden">
        {Editor.line(@editor, @breeze.terminal.width)}
      </box>
      <box :if={not is_nil(@confirmation)} class="block h-4 border">
        <box class="h-1">Read from {@confirmation.owner}</box>
        <box class="h-1">{@confirmation.encoded_bytes} wire bytes · y Confirm · n Cancel</box>
      </box>
      <box
        :if={not is_nil(@overlay)}
        id="overlay"
        focusable
        focus-scope="trap"
        class="w-full border focus:border-6 overflow-scroll"
        style={%{height: @body_height}}
        implicit={Slap.Inspect.DetailsScroll}
        content-width={@overlay_width}
        content-key={@overlay.body}
      >
        <box style={%{width: @overlay_width}}>{@overlay_text}</box>
      </box>
      <box
        :if={not is_nil(@root_chooser)}
        id="root-chooser"
        focusable
        focus-scope="trap"
        class="block w-full border focus:border-6 overflow-hidden"
        style={%{height: @body_height}}
      >
        <box class="h-1 font-bold">Interpret roots</box>
        <box
          :for={{{name, type}, index} <- window(@root_chooser.choices, @root_chooser.index, @body_height - 3)}
          class={if index == @root_chooser.index do
      "h-1 bg-6 text-0"
    else
      "h-1"
    end}
        >
          {root_label(name, type, index == @root_chooser.index, @root_chooser.holds[name])}
        </box>
      </box>
      <box
        :if={is_nil(@root_chooser) and is_nil(@overlay)}
        class={if @wide do
      "grid grid-cols-3"
    else
      "grid grid-cols-1"
    end}
        style={%{width: :full, height: @body_height}}
      >
        <box
          :if={@wide}
          id="navigation"
          class="h-full border focus:border-6 overflow-hidden"
          style={%{width: @navigation_width}}
        >
          <box class="block">
            <box :for={line <- @navigation} class="h-1 overflow-hidden">{line}</box>
          </box>
        </box>
        <box
          :if={not @detail or @wide}
          id="records"
          focusable
          default-focus
          implicit={Slap.Inspect.PaneFocus}
          br-change="pane"
          class="w-full h-full border focus:border-6 overflow-hidden"
        >
          <box class="block">
            <box class="h-1 font-bold">
              {records_title(@rows, @selected, @page_label, @page, not @detail)}
            </box>
            <box :if={@rows == []}>{empty(@page, @operation, @diagnostic, @others)}</box>
            <box
              :for={{row, index} <- visible(@rows, @selected, @body_height - 3)}
              style={%{height: 1}}
              class={if Rows.id(row) == @selected do
      "bg-6 text-0"
    else
      ""
    end}
            >
              {Bytes.head(label(row, index), @record_width)}
            </box>
          </box>
        </box>
        <box
          :if={@detail or @wide}
          id="details"
          focusable={@rows != []}
          class="w-full h-full border focus:border-6 overflow-scroll"
          implicit={Slap.Inspect.DetailsScroll}
          br-change="pane"
          content-width={@detail_width}
          content-key={@detail_key}
          scroll-disabled={not is_nil(@editor) or not is_nil(@confirmation)}
        >
          <box class="block" style={%{width: max(@detail_width, 25)}}>
            <box class="h-1 font-bold">
              Details · {@detail_mode}{if @detail do
                " · Focused"
              else
                ""
              end}
            </box>
            <box>{@detail_text}</box>
          </box>
        </box>
      </box>
      <box :for={line <- @help_lines} class="h-1 w-full overflow-hidden">{line}</box>
    </box>
    """
  end

  defp pane_width(width, navigation) when width >= @two_panes, do: div(width - navigation, 2) - 4
  defp pane_width(width, _navigation), do: width - 4

  defp navigation_width(menu) do
    longest = menu |> short_names() |> Enum.map(&String.length/1) |> Enum.max(fn -> 0 end)
    min(max(longest + 2, @navigation_width.first), @navigation_width.last)
  end

  defp navigation(menu, {_cluster, view} = on_screen) do
    home = if view == :home, do: ">h home", else: " h home"

    lines =
      for {group, name} <- Enum.zip(menu, short_names(menu)),
          line <- [name | Enum.map(group.items, &item(&1, group.cluster, on_screen))],
          do: line

    [home | lines]
  end

  defp short_names(menu) do
    names = for group <- menu, do: group.cluster |> inspect() |> String.split(".")
    shared = names |> Enum.map(&Enum.drop(&1, -1)) |> common_prefix() |> length()
    Enum.map(names, &(&1 |> Enum.drop(shared) |> Enum.join(".")))
  end

  defp common_prefix([_one]), do: []
  defp common_prefix(names), do: Prefix.common(names)

  defp item(%{view: view, key: key}, cluster, on_screen) do
    marker = if {cluster, view} == on_screen, do: ">", else: " "
    "#{marker}#{key || " "} #{view}"
  end

  defp body_height(assigns) do
    confirmation = if assigns.confirmation, do: 4, else: 0
    editing = if assigns.editor, do: 3, else: 0
    max(assigns.breeze.terminal.height - 5 - confirmation - editing, 3)
  end

  defp observation(assigns) do
    refresh =
      cond do
        not Operation.refresh?(assigns.request.operation) -> "Manual read"
        assigns.refresh == 0 -> "Refresh off"
        true -> "Refresh #{assigns.refresh}ms"
      end

    last =
      if assigns.observed_at,
        do: Calendar.strftime(assigns.observed_at, "%H:%M:%S UTC"),
        else: "never"

    stale = if assigns.stale, do: " · Last good data", else: ""
    refresh <> " · Last read " <> last <> stale
  end

  defp text_width(text),
    do: text |> String.split("\n") |> Enum.map(&Utils.string_length/1) |> Enum.max()

  defp overlay_text(nil, _height), do: ""
  defp overlay_text(%{body: text}, _height) when is_binary(text), do: Bytes.display(text)

  defp overlay_text(%{body: {:shards, shards, index}}, height) do
    capacity = height - 5

    columns =
      Enum.map(shards, fn shard ->
        owner = to_string(shard.owner || "unassigned")
        status = if shard.status == :unknown, do: "status unknown", else: "#{shard.status}"
        ["#{shard.shard}", owner, status, Labels.rows(shard[:rows])]
      end)

    widths =
      columns
      |> Enum.zip_with(&Enum.map(&1, fn text -> String.length(text) end))
      |> Enum.map(&Enum.max/1)

    rows =
      columns
      |> window(index, capacity)
      |> Enum.map(fn {cells, at} ->
        marker = if at == index, do: ">", else: " "

        cells =
          cells
          |> Enum.zip(widths)
          |> Enum.map(fn {text, width} -> String.pad_trailing(text, width) end)

        [marker | cells] |> Enum.join("  ") |> String.trim_trailing()
      end)

    Enum.join(
      ["Select shard · one shard per listing", "↑↓ Select · Enter Load · Esc Cancel", "" | rows],
      "\n"
    )
  end

  defp root_label(name, type, selected, holds),
    do:
      if(selected, do: "> ", else: "  ") <>
        String.pad_trailing(to_string(type || :none), 13) <> Bytes.text(name) <> holding(holds)

  defp holding("sequence"), do: "  (holds an array, XML or text containing only embeds)"
  defp holding(nil), do: ""
  defp holding(holds), do: "  (holds #{holds})"

  defp visible(rows, selected, capacity) do
    index = Rows.index(rows, selected) || 0
    window(rows, index, capacity)
  end

  defp window(items, index, capacity) do
    capacity = max(capacity, 1)
    items |> Enum.with_index() |> Enum.drop(max(index - capacity + 1, 0)) |> Enum.take(capacity)
  end

  defp label(row, index), do: "#{index + 1} #{Labels.label(row)}"

  defp title_line(assigns) do
    observation = " · " <> observation(assigns)
    room = assigns.breeze.terminal.width - Utils.string_length(observation)
    Bytes.head(title(assigns), max(room, 0)) <> observation
  end

  defp title(%{request: request} = assigns) do
    if Operation.every_cluster?(request.operation),
      do: "#{request.view} · every cluster",
      else: "#{inspect(assigns.connection.cluster)} · #{request.view} · #{shard_label(assigns)}"
  end

  defp shard_label(assigns) do
    shard = (assigns.page || %{})[:shard] || assigns.request.shard
    group = Enum.find(assigns.menu, &(&1.cluster == assigns.connection.cluster)) || %{}

    case {shard, group[:shards]} do
      {nil, nil} -> "shard not read yet"
      {nil, count} -> "#{count} shards"
      {shard, nil} -> "shard #{shard}"
      {shard, count} -> "shard #{shard} of #{count}"
    end
  end

  defp records_title(rows, selected, label, page, focused) do
    position = if index = Rows.index(rows, selected), do: "#{index + 1}", else: "–"
    more = if is_binary((page || %{})[:cursor]), do: " · n more", else: ""
    focus = if focused, do: " · Focused", else: ""
    "Records · #{position}/#{length(rows)} · #{label}#{more}#{focus}"
  end

  defp page_label(%{paging: :forward, history: history}), do: "page #{length(history) + 1}"

  defp page_label(%{paging: :tail, request: request, rows: rows}) do
    case request.opts[:before] do
      :end -> "near tail"
      nil -> "page from offset #{Rows.first_offset(rows) || "–"}"
      offset -> "page before offset #{offset}"
    end
  end

  defp empty(nil, :idle, nil, _others), do: "Nothing read yet."

  defp empty(nil, :idle, _reason, _others),
    do: "Nothing read: the read failed. e shows diagnostics."

  defp empty(nil, _operation, _reason, _others), do: "Loading"
  defp empty(_page, _operation, _reason, nil), do: "No rows here. s selects another shard."

  defp empty(page, _operation, _reason, counts) do
    if Enum.all?(counts, &(&1.rows == {:exactly, 0})),
      do: "No rows on any shard.",
      else: "No rows on shard #{page[:shard]}. s counts each shard's rows."
  end

  defp status(status, nil, _preview), do: status

  defp status(status, page, preview) do
    label =
      if status == "Ready" and
           (page[:stopped] == :deadline or (preview || %{})[:stopped] == :deadline),
         do: "Partial: deadline reached",
         else: status

    sequence = (preview || page)[:durable_seq]
    metadata = if is_nil(sequence), do: "", else: " · durable seq #{sequence}"
    label <> metadata
  end

  defp presentation(mode) do
    fields = %{editor: nil, overlay: nil, root_chooser: nil, confirmation: nil}

    case mode do
      {:editor, editor, _return_mode} -> %{fields | editor: editor}
      {:overlay, overlay} -> %{fields | overlay: overlay}
      {:roots, chooser} -> %{fields | root_chooser: chooser}
      {:confirm, _request, capture} -> %{fields | confirmation: capture}
      :browse -> fields
    end
  end
end
