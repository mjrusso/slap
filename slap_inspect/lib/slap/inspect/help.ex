defmodule Slap.Inspect.Help do
  @moduledoc false
  alias Slap.Inspect.{CLI, Editor, Links, Operation, Wrap}

  def lines(%{editor: %Editor{}}, _row, _wide),
    do: [
      "←→ Move · Home/End · Backspace/Delete · ↑↓ History",
      "Enter Submit · Tab Complete · Esc Cancel · Ctrl-U Clear"
    ]

  def lines(%{overlay: %{body: {:shards, _, _}}}, _row, _wide),
    do: ["↑↓ Select shard · Enter Load · Esc Cancel", "Each listing reads one selected shard"]

  def lines(%{overlay: overlay}, _row, _wide) when not is_nil(overlay),
    do: [
      "↑↓ Scroll · PgUp/PgDn Page · ←→ Pan · [ ] Pan faster",
      "Esc Return · Home/End Top/Bottom"
    ]

  def lines(%{confirmation: confirmation}, _row, _wide) when not is_nil(confirmation),
    do: [
      "y Confirm read · n / Esc Cancel",
      "Reads run on the shard owner"
    ]

  def lines(%{root_chooser: chooser}, _row, _wide) when not is_nil(chooser),
    do: [
      "↑↓ Root · ←→ Type · Enter Review cost · Esc Cancel",
      "Types: none · text · array · map · xml_fragment"
    ]

  def lines(assigns, row, wide) do
    bytes =
      if row && is_integer(row[:value_size]) && row[:oversized_key] != true,
        do: " · m/M Bytes",
        else: ""

    controls =
      cond do
        assigns.detail -> "↑↓/PgUp/PgDn Scroll · ←→ Pan · d View" <> bytes <> " · Esc List"
        assigns.rows == [] -> "n/p Page · / Command" <> views(wide)
        true -> "↑↓ Select · Enter Details · n/p Page · / Command" <> views(wide)
      end

    back = if assigns.navigation == [], do: [], else: ["b Back"]
    actions = back ++ row_actions(assigns, row)
    common = ["r Read", "t Auto", "s Shards", "? Help", "q Quit"]
    width = assigns.breeze.terminal.width

    extra =
      Enum.reduce(actions, [], fn action, accepted ->
        if String.length(Enum.join([action | accepted] ++ common, " · ")) <= width,
          do: [action | accepted],
          else: accepted
      end)

    [controls, Enum.join(Enum.reverse(extra) ++ common, " · ")]
  end

  # Narrow terminals hide the navigation pane that lists the views.
  defp views(true), do: ""
  defp views(false), do: " · 1-9 Views"

  defp row_actions(assigns, row) do
    operation = assigns.request.operation

    actions =
      if Operation.content_view(operation) && assigns.request.opts[:base],
        do: ["c Content"],
        else: []

    actions = if assigns.request.view == :yjs_content, do: ["i Roots" | actions], else: actions
    actions = if Operation.tail?(operation), do: ["T Tail" | actions], else: actions

    link(Links.follow(row, assigns.request), "f Follow") ++
      link(Links.retention(row), "g Retention") ++ actions
  end

  defp link(:none, _action), do: []
  defp link({:ok, _view, _opts, _shard}, action), do: [action]
  defp link({:open, _cluster, _view, _shard}, action), do: [action]

  defp menu(menu) do
    for group <- menu,
        keyed = for(%{view: view, key: key} <- group.items, key, do: "#{key} #{view}"),
        keyed != [],
        do: inspect(group.cluster) <> ": " <> Enum.join(keyed, " · ")
  end

  defp table(menu, width) do
    sections = [
      {"Records",
       [
         {"↑ ↓", "Select a record"},
         {"Home End", "First or last record"},
         {"Enter", "Open the selected record's details"},
         {"Tab", "Switch between records and details"},
         {"n p", "Next or previous page"},
         {"T",
          "Read near the tail (messages, logical), within query offsets. Large messages may need n to reach the end; p reads backward and b returns"}
       ]},
      {"Details",
       [
         {"↑ ↓ PgUp PgDn", "Scroll"},
         {"Home End", "Top or bottom"},
         {"← → [ ]", "Pan one or twenty columns"},
         {"d", "Cycle through available text, JSON, byte and Elixir term views"},
         {"m M", "Next or previous byte range"},
         {"Esc b", "Close details"}
       ]},
      {"Views",
       [
         {"h", "Home: every cluster's views, with row counts per shard"},
         {"1-9", "A view from the navigation pane; each cluster keeps its own query"}
       ] ++
         Enum.map(menu(menu), &{"", &1}) ++
         [
           {"f", "Follow the selected record's link, or open a view from home"},
           {"g", "Retention of the selected stream"},
           {"b", "Return to the previous query"},
           {"s", "Choose a shard; listings of a whole shard show row counts"},
           {"/", "Edit the query; Enter submits, Esc cancels"}
         ]},
      {"Reading",
       [
         {"r", "Refresh the current view; home and content skip timed refresh"},
         {"t", "Turn timed refresh off or back on"},
         {"e", "Error diagnostics"},
         {"c", "Content: a log's snapshot and later entries, or a Yjs document"},
         {"i", "Yjs root types"}
       ]},
      {"Editor",
       [
         {"← → Home End", "Move"},
         {"↑ ↓", "History"},
         {"Tab", "Complete"},
         {"Ctrl-U", "Clear"}
       ]}
    ]

    keys =
      sections
      |> Enum.flat_map(&elem(&1, 1))
      |> Enum.map(&String.length(elem(&1, 0)))
      |> Enum.max()

    indent = String.duplicate(" ", keys + 4)
    room = max(width - keys - 4, 20)

    Enum.map(sections, fn {title, entries} ->
      lines =
        for {key, text} <- entries do
          [first | rest] =
            text |> Wrap.lines(room) |> String.split("\n") |> Enum.map(&String.trim_trailing/1)

          ["  " <> String.pad_trailing(key, keys + 2) <> first | Enum.map(rest, &(indent <> &1))]
        end

      [title | lines] ++ [""]
    end)
  end

  def text(assigns) do
    page = assigns.page || %{}
    # The overlay's border takes two columns.
    width = assigns.breeze.terminal.width - 2

    Enum.join(
      List.flatten([
        "Keyboard help",
        "",
        "Query: " <> CLI.command(assigns.request),
        "Node: #{assigns.connection.entry}",
        "Cluster: #{inspect(assigns.connection.cluster)}",
        "Owner: #{page[:owner] || "unknown"}",
        "Shard: #{page[:shard] || assigns.request.shard || "not read yet"} (a listing reads one shard; s selects another)",
        "",
        table(assigns.menu, width),
        "Content reads are manual; inputs over 1 MiB ask for confirmation of owner and size.",
        "Last read is the last successful page read; failed refreshes retain last good data.",
        "",
        "Esc returns to the same query, selection and scroll position. q quits."
      ]),
      "\n"
    )
    |> Wrap.lines(width)
  end
end
