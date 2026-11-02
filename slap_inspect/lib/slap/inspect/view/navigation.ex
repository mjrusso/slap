defmodule Slap.Inspect.View.Navigation do
  @moduledoc false
  import Breeze.View, only: [focus: 2]
  import Breeze.Component, only: [assign: 2]

  alias Slap.Inspect.{CLI, Clusters, Details, Feedback, Links, Request, Rows}
  alias Slap.Inspect.View.{Dialogs, Read}

  def display_mode(term) do
    mode =
      case term.assigns.display do
        :bytes -> :text
        :text -> :json
        :json -> :bytes
      end

    {:noreply, assign(term, display: mode)}
  end

  def select(term, delta) do
    rows = term.assigns.rows
    at = Rows.index(rows, term.assigns.selected) || -1
    row = Enum.at(rows, max(min(at + delta, length(rows) - 1), 0))
    assign(term, selected: row && Rows.id(row), preview: nil, byte_history: [])
  end

  def next_page(%{assigns: %{page: %{cursor: cursor}}} = term) when is_binary(cursor) do
    request = term.assigns.request

    history = [
      {request.opts[:cursor], request.opts[:cursor_exclusive] || false} | term.assigns.history
    ]

    {:noreply,
     term
     |> assign(
       request: %{
         request
         | opts:
             Keyword.merge(request.opts,
               cursor: cursor,
               cursor_exclusive: term.assigns.page[:cursor_exclusive] || false
             )
       },
       history: history
     )
     |> Read.page()}
  end

  def next_page(term), do: {:noreply, term}

  def previous_page(%{assigns: %{history: [{cursor, exclusive} | rest]}} = term) do
    request = term.assigns.request

    opts =
      if cursor,
        do: Keyword.merge(request.opts, cursor: cursor, cursor_exclusive: exclusive),
        else: Keyword.drop(request.opts, [:cursor, :cursor_exclusive])

    {:noreply, term |> assign(request: %{request | opts: opts}, history: rest) |> Read.page()}
  end

  def previous_page(term), do: {:noreply, term}

  def back(%{assigns: %{detail: true}} = term),
    do: {:noreply, term |> assign(detail: false) |> focus("records")}

  def back(%{assigns: %{navigation: [{request, history} | rest]}} = term),
    do:
      {:noreply,
       term |> assign(request: request, navigation: rest, history: history) |> Read.page()}

  def back(term), do: {:noreply, term}

  def more(term) do
    case Rows.selected(term.assigns) do
      %{oversized_key: false} = row ->
        preview = term.assigns.preview || row
        length = Details.range_size(preview, term.assigns.display)
        offset = (preview[:offset] || 0) + length

        if offset >= preview.value_size do
          {:noreply, assign(term, status: "End of value")}
        else
          previous = {preview[:offset] || 0, length}

          history = remember_range(previous, term.assigns.byte_history)

          Read.fetch_bytes(term, row, offset, history)
        end

      _ ->
        {:noreply, term}
    end
  end

  defp remember_range({_offset, 0}, history), do: history
  defp remember_range(range, history), do: [range | history]

  def previous_bytes(%{assigns: %{byte_history: [range | rest]}} = term) do
    case Rows.selected(term.assigns) do
      nil -> {:noreply, term}
      row -> Read.fetch_bytes(term, row, range, rest)
    end
  end

  def previous_bytes(term), do: {:noreply, term}

  def follow(term) do
    case Links.follow(Rows.selected(term.assigns), term.assigns.request) do
      {:ok, view, opts, shard} -> navigate(term, view, opts, shard)
      :none -> {:noreply, assign(term, status: "This row has no source link")}
    end
  end

  def content(term) do
    case term.assigns.request.opts[:base] do
      nil ->
        {:noreply, assign(term, status: "Use yjs --content --base PATH --root TYPE:NAME")}

      _base ->
        page = term.assigns.page || %{}
        shard = get_in(page, [:metadata, :shard]) || page[:shard] || term.assigns.request.shard

        opts =
          term.assigns.request.opts
          |> Keyword.take([
            :base,
            :roots,
            :placement_key,
            :max_input_bytes,
            :max_entries,
            :max_output_bytes
          ])

        navigate(term, :yjs_content, opts, shard)
    end
  end

  def retention(term) do
    case Links.retention(Rows.selected(term.assigns)) do
      {:ok, view, opts, shard} -> navigate(term, view, opts, shard)
      :none -> {:noreply, assign(term, status: "Select stream metadata to inspect retention")}
    end
  end

  defp navigate(term, view, opts, shard) do
    request = term.assigns.request
    page = term.assigns.page || %{}

    shard = shard || page[:shard] || request.shard
    {:ok, next} = Request.derive(%{request | shard: shard}, view, opts)

    {:noreply,
     term
     |> assign(
       request: next,
       navigation: [{request, term.assigns.history} | term.assigns.navigation],
       history: []
     )
     |> Read.page()}
  end

  # Defaults of the assigns saved per cluster. An assign that belongs to a
  # query must be listed here, or it carries over when another cluster opens.
  @browsing %{
    page: nil,
    rows: [],
    selected: nil,
    detail: false,
    preview: nil,
    byte_history: [],
    observed_at: nil,
    stale: false,
    history: [],
    navigation: [],
    status: "Loading",
    diagnostic: nil,
    operation: :idle,
    id: nil
  }

  def browsing(request), do: Map.put(@browsing, :request, request)

  @doc "Opens the view under `key`. Opening the view on screen reads it again."
  def open(term, key) do
    case Clusters.find(term.assigns.menu, key) do
      nil -> {:noreply, term}
      {cluster, view} -> {:noreply, term |> select_cluster(cluster) |> enter(view)}
    end
  end

  defp select_cluster(%{assigns: %{connection: %{cluster: cluster}}} = term, cluster), do: term

  defp select_cluster(term, cluster) do
    connection = term.assigns.connection
    current = Map.take(term.assigns, [:request | Map.keys(@browsing)])
    saved = Map.put(term.assigns.saved, connection.cluster, current)
    state = Map.get_lazy(saved, cluster, &fresh/0)

    # A read in flight when the state was saved no longer updates it: the
    # request worker drops its result if another read is submitted, and the
    # view ignores it otherwise, since its id is no longer current.
    term
    |> assign(Map.to_list(state))
    |> assign(saved: saved, connection: %{connection | cluster: cluster}, operation: :idle)
    |> focus(if state.detail, do: "details", else: "records")
  end

  defp fresh do
    {:ok, request} = Request.new(%{view: :raw, shard: nil, budget: nil, opts: []})
    browsing(request)
  end

  defp enter(%{assigns: %{request: %{view: view}}} = term, view), do: resume(term)

  # A view that needs arguments opens the editor over the cluster's last
  # results, without reading.
  defp enter(term, view) do
    candidate = Request.draft(term.assigns.request, view)

    case Request.new(candidate) do
      {:ok, request} ->
        term |> assign(request: request, history: []) |> Read.page()

      {:error, reason} ->
        term
        |> Dialogs.editor(prefill(CLI.command(candidate), candidate.opts, reason))
        |> assign(diagnostic: reason, status: Feedback.error(reason))
    end
  end

  defp prefill(command, opts, {:required_options, [first | _]}) do
    case Enum.find(first, &is_nil(opts[&1])) do
      nil -> command
      key -> "#{command} #{Feedback.option(key)} "
    end
  end

  defp prefill(command, _opts, _reason), do: command

  # Yjs content is reconstructed only on request.
  defp resume(%{assigns: %{request: %{view: :yjs_content}}} = term), do: term
  defp resume(term), do: Read.start(term)
end
