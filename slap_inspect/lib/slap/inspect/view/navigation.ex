defmodule Slap.Inspect.View.Navigation do
  @moduledoc false
  import Breeze.View, only: [focus: 2]
  import Breeze.Component, only: [assign: 2]

  alias Slap.Inspect.{Clusters, Details, Links, Operation, Request, Rows}
  alias Slap.Inspect.View.Read

  @doc "Changes the display to the next view that applies to the selected record."
  def display_mode(%{assigns: %{display: display} = assigns} = term) do
    case Rows.selected(assigns) do
      nil ->
        {:noreply, term}

      row ->
        modes = Details.view(row, assigns.preview).modes
        at = Enum.find_index(modes, &(&1 == Details.shown(modes, display)))
        next = Enum.at(modes, rem(at + 1, length(modes)))
        # Keep :text as the preference so selecting a text record restores text mode.
        {:noreply, assign(term, display: if(next == :summary, do: :text, else: next))}
    end
  end

  def select(term, delta) do
    rows = term.assigns.rows
    at = Rows.index(rows, term.assigns.selected) || -1
    row = Enum.at(rows, max(min(at + delta, length(rows) - 1), 0))
    assign(term, selected: row && Rows.id(row), preview: nil, byte_history: [])
  end

  def next_page(%{assigns: %{page: %{cursor: cursor} = page}} = term) when is_binary(cursor) do
    request = term.assigns.request

    {:noreply,
     term
     |> assign(
       request: Request.next(request, page),
       history: [request.opts | term.assigns.history]
     )
     |> Read.page()}
  end

  def next_page(%{assigns: %{page: %{}}} = term),
    do: {:noreply, assign(term, status: "This is the last page")}

  def next_page(term), do: {:noreply, term}

  def previous_page(%{assigns: %{history: [opts | rest]}} = term),
    do:
      {:noreply,
       term |> assign(request: %{term.assigns.request | opts: opts}, history: rest) |> Read.page()}

  def previous_page(%{assigns: %{page: %{} = page, request: request}} = term) do
    offset = Rows.first_offset(term.assigns.rows)

    if Operation.tail?(request.operation) and is_integer(offset) and
         offset > lower_bound(page, request),
       do: read_before(term, offset),
       else: {:noreply, assign(term, status: "This is the first page")}
  end

  def previous_page(term), do: {:noreply, term}

  def close_details(%{assigns: %{detail: true}} = term),
    do: {:noreply, term |> assign(detail: false) |> focus("records")}

  def close_details(term), do: {:noreply, term}

  def back(%{assigns: %{detail: true}} = term), do: close_details(term)

  def back(%{assigns: %{navigation: [{request, history, selected, paging} | rest]}} = term),
    do:
      {:noreply,
       term
       |> assign(request: request, navigation: rest, history: history, paging: paging)
       |> Read.page()
       |> assign(selected: selected)}

  def back(term), do: {:noreply, term}

  def more(term) do
    case Rows.selected(term.assigns) do
      %{oversized_key: false} = row ->
        preview = term.assigns.preview || row

        mode =
          row
          |> Details.view(term.assigns.preview)
          |> Map.fetch!(:modes)
          |> Details.shown(term.assigns.display)

        length = Details.range_step(preview, mode)
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
    case Rows.selected(term.assigns) &&
           Links.follow(Rows.selected(term.assigns), term.assigns.request) do
      nil -> no_record(term)
      {:ok, view, opts, shard} -> navigate(term, view, opts, shard)
      {:open, cluster, view, shard} -> open_view(term, cluster, view, shard)
      :none -> {:noreply, assign(term, status: "This row has no source link")}
    end
  end

  @doc """
  Reads from a message boundary near the stream's tail or the query's
  `:until` bound and selects the newest returned message. Large messages
  may require forward pages to reach that bound. `p` reads backward,
  and `b` returns to the query.
  """
  def tail(%{assigns: %{request: request}} = term) do
    cond do
      not Operation.tail?(request.operation) ->
        {:noreply, assign(term, status: "T reads the tail in the messages and logical views")}

      # Repeated T presses must preserve the original query for b.
      term.assigns.paging == :tail ->
        read_before(term, :end)

      true ->
        term |> assign(navigation: [entry(term) | term.assigns.navigation]) |> read_before(:end)
    end
  end

  defp read_before(term, before) do
    request = term.assigns.request

    opts =
      request.opts |> Keyword.drop([:cursor, :cursor_exclusive]) |> Keyword.put(:before, before)

    {:noreply,
     term |> assign(request: %{request | opts: opts}, history: [], paging: :tail) |> Read.page()}
  end

  defp entry(%{assigns: assigns}),
    do: {assigns.request, assigns.history, assigns.selected, assigns.paging}

  # Backward pages report their chosen start as `from`. Use the stream's
  # lower bound to decide whether earlier pages can still be read.
  defp lower_bound(page, request),
    do:
      get_in(page, [:metadata, :logical, :from]) ||
        max(Keyword.get(request.opts, :from, 0), get_in(page, [:offsets, :trim]) || 0)

  def content(term) do
    request = term.assigns.request

    case {Operation.content_view(request.operation), request.opts[:base]} do
      {nil, _base} ->
        {:noreply, assign(term, status: "c reads content in the log and yjs views")}

      {_view, nil} ->
        {:noreply,
         assign(term, status: "Use log --content --base PATH or yjs --content --base PATH")}

      {view, _base} ->
        page = term.assigns.page || %{}
        shard = get_in(page, [:metadata, :shard]) || page[:shard] || request.shard

        opts =
          Keyword.take(request.opts, [
            :base,
            :roots,
            :placement_key,
            :max_input_bytes,
            :max_entries,
            :max_output_bytes
          ])

        navigate(term, view, opts, shard)
    end
  end

  def retention(term), do: retention(term, Rows.selected(term.assigns))

  defp retention(term, nil), do: no_record(term)

  defp retention(term, row) do
    case Links.retention(row) do
      {:ok, view, opts, shard} -> navigate(term, view, opts, shard)
      :none -> {:noreply, assign(term, status: "Select stream metadata to inspect retention")}
    end
  end

  defp no_record(term), do: {:noreply, assign(term, status: "No record is selected")}

  defp navigate(term, view, opts, shard) do
    request = term.assigns.request
    page = term.assigns.page || %{}

    shard = shard || page[:shard] || request.shard
    budget = Request.chosen_budget(request)
    {:ok, next} = Request.derive(%{request | shard: shard, budget: budget}, view, opts)

    {:noreply,
     term
     |> assign(
       request: next,
       navigation: [entry(term) | term.assigns.navigation],
       history: [],
       paging: :forward
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
    paging: :forward,
    navigation: [],
    status: "Loading",
    diagnostic: nil,
    operation: :idle,
    id: nil,
    pending_keys: nil,
    approved: nil,
    others: nil
  }

  def browsing(request), do: Map.put(@browsing, :request, request)

  @doc "Opens the view under `key`. Opening the view on screen reads it again."
  def open(term, key) do
    case Clusters.find(term.assigns.menu, key) do
      nil -> {:noreply, term}
      {cluster, view} -> open_view(term, cluster, view)
    end
  end

  def open_view(term, cluster, view, shard \\ nil),
    do: {:noreply, term |> select_cluster(cluster) |> enter(view, shard)}

  def home(term), do: {:noreply, enter(term, :home)}

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

  defp enter(term, view, shard \\ nil)

  defp enter(%{assigns: %{request: %{view: view, shard: current}}} = term, view, shard)
       when is_nil(shard) or shard == current,
       do: resume(term)

  defp enter(term, view, shard) do
    draft = Request.draft(term.assigns.request, view)
    {:ok, request} = Request.new(%{draft | shard: shard || draft.shard})
    term |> assign(request: request, history: [], paging: :forward) |> Read.page()
  end

  defp resume(term) do
    if Operation.manual?(term.assigns.request.operation), do: term, else: Read.start(term)
  end
end
