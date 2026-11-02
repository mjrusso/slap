defmodule Slap.Inspect.View.Read do
  @moduledoc false
  import Breeze.View, only: [focus: 2]
  import Breeze.Component, only: [assign: 2]

  alias Slap.Inspect.{Feedback, Request, Requests, Rows}

  def start(term), do: start(term, term.assigns.request)

  def start(term, request) do
    case Request.new(request) do
      {:ok, request} ->
        start_validated(term, request)

      {:error, reason} ->
        assign(term, operation: :idle, diagnostic: reason, status: Feedback.error(reason))
    end
  end

  def submit(term, id, request),
    do: Requests.submit(term.assigns.worker, id, term.assigns.connection, request)

  defp start_validated(term, request) do
    {id, submitted, operation} =
      if request.view == :yjs_content,
        do:
          {{:preflight, make_ref()},
           %{request | opts: Keyword.put(request.opts, :preflight, true)}, {:preflight, request}},
        else: {make_ref(), request, :reading}

    submit(term, id, submitted)

    assign(term,
      id: id,
      operation: operation,
      status: "Loading",
      preview: nil,
      mode: :browse,
      diagnostic: nil
    )
  end

  def page(term) do
    term
    |> assign(
      page: nil,
      rows: [],
      selected: nil,
      detail: false,
      observed_at: nil,
      stale: false,
      byte_history: []
    )
    |> focus("records")
    |> start()
  end

  def toggle_refresh(term) do
    refresh = if term.assigns.refresh == 0, do: 5000, else: 0
    assign(term, refresh: refresh)
  end

  defp refreshable?(%{refresh: interval, request: %{view: view}, operation: :idle, mode: :browse})
       when interval > 0 and view != :yjs_content, do: true

  defp refreshable?(_assigns), do: false

  # The view calls this after every update: one timer runs exactly while the
  # view is refreshable.
  def sync_refresh(%{assigns: %{refresh_timer: nil} = assigns} = term) do
    if refreshable?(assigns),
      do:
        assign(term,
          refresh_timer: Process.send_after(self(), {:refresh, assigns.id}, assigns.refresh)
        ),
      else: term
  end

  def sync_refresh(%{assigns: %{refresh_timer: timer} = assigns} = term) do
    if refreshable?(assigns) do
      term
    else
      Process.cancel_timer(timer)
      assign(term, refresh_timer: nil)
    end
  end

  def handle_info(
        {:inspection_result, {:shards, _} = id, {:ok, shards}},
        %{assigns: %{id: id}} = term
      ) do
    current = (term.assigns.page || %{})[:shard] || term.assigns.request.shard

    index = Enum.find_index(shards, &(&1 == current)) || 0

    overlay = {:overlay, %{body: {:shards, shards, index}, focus: term.focused, return: :browse}}

    mode =
      case term.assigns.mode do
        {:editor, editor, _return_mode} -> {:editor, editor, overlay}
        _ -> overlay
      end

    {:noreply,
     term |> assign(operation: :idle, status: "Select shard", mode: mode) |> focus("overlay")}
  end

  def handle_info(
        {:inspection_result, {:preflight, _} = id, {:ok, capture}},
        %{assigns: %{id: id, operation: operation}} = term
      ) do
    case operation do
      {:preflight, request} ->
        {:noreply,
         assign(term,
           operation: :idle,
           status: "Confirm reconstruction",
           mode: beneath(term.assigns.mode, {:confirm, request, capture})
         )}

      _ ->
        {:noreply, term}
    end
  end

  def handle_info(
        {:inspection_result, {:preview, key, history, _} = id, {:ok, page}},
        %{assigns: %{id: id}} = term
      ) do
    term =
      assign(term,
        operation: :idle,
        status: "Ready",
        observed_at: DateTime.utc_now(),
        stale: false,
        diagnostic: nil
      )

    term =
      case {term.assigns.selected == key, page[:row] || page[:object]} do
        {true, nil} ->
          term
          |> assign(
            rows: Enum.reject(term.assigns.rows, &(Rows.id(&1) == key)),
            selected: nil,
            preview: nil,
            status: "Selected record is no longer present"
          )
          |> restore_selection(nil)

        {true, preview} ->
          assign(term,
            preview: Map.merge(preview, Map.take(page, [:stopped, :durable_seq])),
            byte_history: history
          )

        {false, _} ->
          term
      end

    {:noreply, term}
  end

  def handle_info({:inspection_result, id, {:ok, page}}, %{assigns: %{id: id}} = term) do
    rows = rows(page)

    selected =
      if Rows.find(rows, term.assigns.selected),
        do: term.assigns.selected,
        else: nil

    {:noreply,
     term
     |> assign(
       page: page,
       rows: rows,
       selected: selected,
       operation: :idle,
       status:
         if(term.assigns.selected && is_nil(selected),
           do: "Selected record is no longer present",
           else: "Ready"
         ),
       observed_at: DateTime.utc_now(),
       stale: false,
       diagnostic: nil
     )
     |> restore_selection(selected)}
  end

  def handle_info({:inspection_result, id, {:error, reason}}, %{assigns: %{id: id}} = term),
    do:
      {:noreply,
       term
       |> assign(
         operation: :idle,
         stale: not is_nil(term.assigns.page),
         diagnostic: reason,
         status: Feedback.error(reason)
       )}

  def handle_info({:inspection_owner, _id, owner, nil}, term),
    do: {:noreply, assign(term, status: "Reading on #{owner}")}

  def handle_info({:inspection_owner, _id, owner, shard}, term),
    do: {:noreply, assign(term, status: "Reading owner #{owner}, shard #{shard}")}

  def handle_info({:inspection_stalled, _id, owner, nil}, term),
    do: {:noreply, assign(term, status: "Read on #{owner} stalled. Waiting; q quits.")}

  def handle_info({:inspection_stalled, _id, owner, shard}, term),
    do:
      {:noreply,
       assign(term, status: "Owner #{owner} read stalled on shard #{shard}. Waiting; q quits.")}

  def handle_info({:inspection_stalled, _id}, term),
    do:
      {:noreply,
       assign(term,
         status:
           "Owner read stalled on shard #{inspect(term.assigns.request.shard)}. Waiting; q quits."
       )}

  def handle_info({:refresh, id}, %{assigns: %{id: id}} = term) do
    term = assign(term, refresh_timer: nil)
    if refreshable?(term.assigns), do: refresh(term), else: {:noreply, term}
  end

  def handle_info(_message, term), do: {:noreply, term}

  defp restore_selection(term, nil),
    do: term |> assign(detail: false, byte_history: []) |> focus("records")

  defp restore_selection(term, _selected), do: term

  defp rows(%{rows: rows} = page) do
    overview =
      Map.take(page, [
        :metadata,
        :lineage,
        :soft_deleted,
        :creation,
        :log,
        :tail,
        :trim,
        :reclamation,
        :deleted,
        :interpretation
      ])

    if map_size(overview) == 0, do: rows, else: [overview(overview) | rows]
  end

  defp rows(%{object: nil}), do: []

  defp rows(%{object: object}),
    do: [Map.merge(object, %{decoded: {:ok, %{kind: :object}}, oversized_key: false})]

  defp rows(%{row: nil}), do: []
  defp rows(%{row: row}), do: [row]
  defp rows(page), do: [overview(page)]

  defp overview(fields),
    do: %{key: <<>>, value: <<>>, decoded: {:ok, %{kind: :overview, value: fields}}}

  def refresh(%{assigns: %{preview: nil}} = term), do: {:noreply, start(term)}

  def refresh(term) do
    preview = term.assigns.preview
    row = Rows.selected(term.assigns)
    offset = preview[:offset] || 0
    range = if byte_size(preview.value) == 0, do: offset, else: {offset, byte_size(preview.value)}
    fetch_bytes(term, row, range, term.assigns.byte_history)
  end

  def fetch_bytes(term, row, range, history) do
    request = %{term.assigns.request | shard: Map.get(term.assigns.page, :shard)}

    case Request.bytes(request, row.key, range) do
      {:ok, request} ->
        id = {:preview, Rows.id(row), history, make_ref()}
        submit(term, id, request)

        {:noreply,
         assign(term,
           id: id,
           operation: :reading,
           status: "Loading bytes",
           diagnostic: nil
         )}

      {:error, reason} ->
        {:noreply, assign(term, diagnostic: reason, status: Feedback.error(reason))}
    end
  end

  # A confirmation that arrives while the editor or an overlay is open is
  # shown when it closes.
  defp beneath({:editor, editor, _return_mode}, mode), do: {:editor, editor, mode}
  defp beneath({:overlay, overlay}, mode), do: {:overlay, %{overlay | return: mode}}
  defp beneath(_mode, mode), do: mode
end
