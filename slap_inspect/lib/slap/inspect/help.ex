defmodule Slap.Inspect.Help do
  @moduledoc false
  alias Slap.Inspect.{CLI, Editor, Links}

  def lines(%{editor: %Editor{}}, _row),
    do: [
      "←→ Move · Home/End · Backspace/Delete · ↑↓ History",
      "Enter Submit · Tab Complete · Esc Cancel · Ctrl-U Clear"
    ]

  def lines(%{overlay: %{body: {:shards, _, _}}}, _row),
    do: ["↑↓ Select shard · Enter Load · Esc Cancel", "Each listing reads one selected shard"]

  def lines(%{overlay: overlay}, _row) when not is_nil(overlay),
    do: [
      "↑↓ Scroll · PgUp/PgDn Page · ←→ Pan · [ ] Pan faster",
      "Esc Return · Home/End Top/Bottom"
    ]

  def lines(%{confirmation: confirmation}, _row) when not is_nil(confirmation),
    do: [
      "y Confirm reconstruction · n / Esc Cancel",
      "Reads run on the owner; the entry node reconstructs"
    ]

  def lines(%{root_chooser: chooser}, _row) when not is_nil(chooser),
    do: [
      "↑↓ Root · ←→ Type · Enter Review cost · Esc Cancel",
      "Types: none · text · array · map · xml_fragment"
    ]

  def lines(assigns, row) do
    bytes =
      if row && is_integer(row[:value_size]) && row[:oversized_key] != true,
        do: " · m/M Bytes",
        else: ""

    controls =
      if assigns.detail,
        do: "↑↓/PgUp/PgDn Scroll · ←→ Pan · d View" <> bytes <> " · Esc List",
        else: "↑↓ Select · Enter Details · n/p Page · / Command · ? Help"

    actions = row_actions(assigns, row)
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

  defp row_actions(assigns, row) do
    actions = if assigns.request.view in [:log, :yjs, :yjs_content], do: ["c Content"], else: []

    actions = if assigns.request.view == :yjs_content, do: ["i Roots" | actions], else: actions

    link(Links.follow(row, assigns.request), "f Follow") ++
      link(Links.retention(row), "g Retention") ++ actions
  end

  defp link(:none, _action), do: []
  defp link({:ok, _view, _opts, _shard}, action), do: [action]

  defp menu(menu) do
    for group <- menu,
        keyed = for(%{view: view, key: key} <- group.items, key, do: "#{key} #{view}"),
        keyed != [],
        do: inspect(group.cluster) <> ": " <> Enum.join(keyed, " · ")
  end

  def text(assigns) do
    page = assigns.page || %{}

    Enum.join(
      List.flatten([
        "Keyboard help",
        "",
        "Query: " <> CLI.command(assigns.request),
        "Node: #{assigns.connection.entry}",
        "Cluster: #{inspect(assigns.connection.cluster)}",
        "Owner: #{page[:owner] || "unknown"}",
        "Shard: #{page[:shard] || assigns.request.shard || "auto"} (one shard per listing; s selects another)",
        "",
        "Records: ↑↓ select, Enter opens the first or selected record, Tab changes pane.",
        "Details: ↑↓ / PageUp/PageDown scroll; Home/End top/bottom.",
        "←→ pan one column; [ / ] pan twenty columns.",
        "d cycles Bytes, Text and JSON; Text and JSON wrap at the pane width.",
        "m loads the next byte range; M returns to the previous range.",
        "n/p next/previous record page; Esc/b back to list or previous query.",
        "/ edits the current query; only Enter submits; Esc cancels.",
        "Editor: ←→, Home/End, Backspace/Delete, ↑↓ history, Tab completes, Ctrl-U clear.",
        "1-9 open the views listed by cluster in the navigation pane;",
        "each cluster keeps its own query, selection and history.",
        menu(assigns.menu),
        "s lists available shards; Enter selects one; Esc cancels.",
        "r refresh; t toggles five-second refresh; e shows error diagnostics.",
        "f follows linked streams/objects; g shows stream retention.",
        "c reconstructs content; i chooses Yjs root interpretations.",
        "Yjs reconstruction is manual and requires confirmation of owner and wire bytes.",
        "Last read is the last successful page read; failed refreshes retain last good data.",
        "",
        "Esc returns to the same query, selection and scroll position."
      ]),
      "\n"
    )
  end
end
