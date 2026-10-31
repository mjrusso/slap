defmodule Slap.Inspect.View.Layout do
  @moduledoc false
  use Breeze.Component

  alias BackBreeze.Utils
  alias Slap.Inspect.{Bytes, CLI, Details, Editor, Help, Operation, Rows}

  # The terminal width from which navigation, records and details share the screen.
  @two_panes 110

  def prepare(term) do
    assigns = term.assigns
    width = term.terminal.size.width
    panes = presentation(assigns.mode)

    visible =
      is_nil(panes.overlay) and is_nil(panes.root_chooser) and
        (assigns.detail or width >= @two_panes)

    if visible do
      row = Rows.selected(assigns)
      width = pane_width(width)
      key = {row, assigns.preview, assigns.display, width}

      case assigns.detail_cache do
        %{key: ^key} ->
          term

        _ ->
          text = Details.render(row, assigns.preview, assigns.display, width)
          assign(term, detail_cache: %{key: key, text: text, width: text_width(text)})
      end
    else
      term
    end
  end

  def render(assigns) do
    assigns = assign(assigns, presentation(assigns.mode))
    row = Rows.selected(assigns)
    width = pane_width(assigns.breeze.terminal.width)
    details = assigns.detail_cache || %{text: "", width: 0}
    height = body_height(assigns)
    overlay = overlay_text(assigns.overlay, height)

    assigns =
      assign(assigns,
        detail_text: details.text,
        detail_width: details.width,
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
        shard_label: (assigns.page || %{})[:shard] || assigns.request.shard || "auto",
        observation: observation(assigns),
        help_lines: Help.lines(assigns, row),
        body_height: height
      )

    ~H"""
    <box class="block w-screen h-screen">
      <box class="h-1 w-full font-bold overflow-hidden">
        Slap Inspect · {@request.view} · shard {@shard_label} (one shard)
      </box>
      <box class="h-1 w-full overflow-hidden">{@command}</box>
      <box class="h-1 w-full overflow-hidden">{@observation}</box>
      <box class="h-1 w-full overflow-hidden">{status(@status, @page, @preview)}</box>
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
          {root_label(name, type)}
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
        <box :if={@wide} id="navigation" class="w-17 h-full border focus:border-6">
          <box class="block">
            <box :for={{view, index} <- Enum.with_index(Operation.views(), 1)}>{index} {view}</box>
            <box>[/] Command</box>
            <box>[?] Help</box>
            <box>[b] Back</box>
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
              {if @detail do
                "Records"
              else
                "Records · Focused"
              end}
            </box>
            <box :if={@rows == []}>No rows on this page. / edits the query.</box>
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
              Details · {@display}{if @detail do
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

  defp pane_width(width) when width >= @two_panes, do: div(width - 17, 2) - 4
  defp pane_width(width), do: width - 4

  defp body_height(assigns) do
    confirmation = if assigns.confirmation, do: 4, else: 0
    editing = if assigns.editor, do: 3, else: 0
    max(assigns.breeze.terminal.height - 6 - confirmation - editing, 3)
  end

  defp observation(assigns) do
    refresh =
      case {assigns.request.view, assigns.refresh} do
        {:yjs_content, _} -> "Manual reconstruction"
        {_, 0} -> "Refresh off"
        {_, interval} -> "Refresh #{interval}ms"
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

    rows =
      shards
      |> window(index, capacity)
      |> Enum.map(fn {shard, at} ->
        if at == index, do: "> #{shard}", else: "  #{shard}"
      end)

    Enum.join(
      ["Select shard · one shard per listing", "↑↓ Select · Enter Load · Esc Cancel", "" | rows],
      "\n"
    )
  end

  defp root_label(name, type),
    do: String.pad_trailing(to_string(type || :none), 13) <> Bytes.text(name)

  defp visible(rows, selected, capacity) do
    index = Rows.index(rows, selected) || 0
    window(rows, index, capacity)
  end

  defp window(items, index, capacity) do
    capacity = max(capacity, 1)
    items |> Enum.with_index() |> Enum.drop(max(index - capacity + 1, 0)) |> Enum.take(capacity)
  end

  defp label(row, index) do
    case row[:decoded] do
      {:ok, %{kind: :row, partition: partition, key: key}} ->
        "#{index + 1} #{Bytes.text(partition)} / #{Bytes.text(key)}"

      {:ok, %{path: path}} ->
        "#{index + 1} #{Bytes.text(path)}"

      {:ok, %{kind: kind} = fields} ->
        "#{index + 1} #{kind} #{Bytes.text(fields[:key] || row.key)}"

      _ ->
        "#{index + 1} #{Bytes.text(row.key)}"
    end
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
