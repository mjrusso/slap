defmodule Slap.Inspect.View.Read do
  @moduledoc false
  import Breeze.View, only: [focus: 2]
  import Breeze.Component, only: [assign: 2]

  alias Slap.Inspect.{Clusters, Feedback, Operation, Request, Requests, Rows}
  alias Slap.Inspect.View.Activity

  @reconnect 2000
  @confirm_above 1024 * 1024

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
      if Operation.manual?(request.operation),
        do:
          {{:preflight, make_ref()},
           %{request | opts: Keyword.put(request.opts, :preflight, true)}, {:preflight, request}},
        else: {make_ref(), count_others(request), :reading}

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

  defp count_others(%Request{shard: nil, operation: operation} = request) do
    if Operation.spans_shard?(operation) and not Keyword.has_key?(request.opts, :cursor),
      do: %{request | count_others: true},
      else: request
  end

  defp count_others(request), do: request

  def page(term) do
    term
    |> assign(
      page: nil,
      rows: [],
      selected: nil,
      detail: false,
      observed_at: nil,
      stale: false,
      byte_history: [],
      others: nil
    )
    |> focus("records")
    |> start()
  end

  def toggle_refresh(term) do
    refresh = if term.assigns.refresh == 0, do: term.assigns.refresh_interval, else: 0
    assign(term, refresh: refresh)
  end

  defp refreshable?(%{operation: :idle, mode: :browse} = assigns), do: interval(assigns) > 0
  defp refreshable?(_assigns), do: false

  # Retry even with timed refresh disabled so disconnected views can reconnect.
  defp interval(%{diagnostic: :disconnected}), do: @reconnect
  defp interval(%{diagnostic: {:disconnected, _node}}), do: @reconnect

  defp interval(%{refresh: refresh, request: request}),
    do: if(Operation.refresh?(request.operation), do: refresh, else: 0)

  # The view calls this after every update: one timer runs exactly while the
  # view is refreshable.
  def sync_refresh(%{assigns: %{refresh_timer: nil} = assigns} = term) do
    if refreshable?(assigns),
      do:
        assign(term,
          refresh_timer: Process.send_after(self(), {:refresh, assigns.id}, interval(assigns))
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

    index = Enum.find_index(shards, &(&1.shard == current)) || 0

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
        {:noreply, confirm_or_read(term, request, capture)}

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
    term = term |> settle_shard(page) |> discovered(page)

    selected =
      if Rows.find(rows, term.assigns.selected),
        do: term.assigns.selected,
        else: nil

    {:noreply,
     term
     |> assign(
       page: page,
       rows: rows,
       others: page[:others] || term.assigns.others,
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
     |> restore_selection(selected)
     |> reslice()
     |> open_record()
     |> select_default()
     |> moved_status(page)}
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
    do: stalled(term, "Read on #{owner} stalled. Waiting; q quits.")

  def handle_info({:inspection_stalled, _id, owner, shard}, term),
    do: stalled(term, "Owner #{owner} read stalled on shard #{shard}. Waiting; q quits.")

  def handle_info({:inspection_stalled, _id}, term),
    do:
      stalled(
        term,
        "Owner read stalled on shard #{inspect(term.assigns.request.shard)}. Waiting; q quits."
      )

  def handle_info({:refresh, id}, %{assigns: %{id: id}} = term) do
    term = assign(term, refresh_timer: nil)
    if refreshable?(term.assigns), do: refresh(term), else: {:noreply, term}
  end

  def handle_info(_message, term), do: {:noreply, term}

  defp stalled(term, status),
    do: {:noreply, term |> assign(status: status) |> Activity.stalled()}

  # Saving the chosen shard prevents refreshes from repeating automatic shard selection.
  defp settle_shard(%{assigns: %{request: %{shard: nil} = request}} = term, %{shard: shard})
       when is_integer(shard) do
    if Operation.spans_shard?(request.operation),
      do: assign(term, request: %{request | shard: shard}),
      else: term
  end

  defp settle_shard(term, _page), do: term

  defp discovered(term, %{clusters: clusters}), do: assign(term, menu: Clusters.menu(clusters))
  defp discovered(term, _page), do: term

  defp moved_status(term, %{moved_from: from, shard: shard}),
    do: assign(term, status: "Shard #{from} has no rows; showing shard #{shard}")

  defp moved_status(term, _page), do: term

  defp open_record(
         %{assigns: %{rows: [row], selected: nil, request: %Request{operation: operation}}} = term
       ) do
    if Operation.listing?(operation),
      do: term,
      else: term |> assign(selected: Rows.id(row), detail: true) |> focus("details")
  end

  defp open_record(term), do: term

  defp select_default(%{assigns: %{selected: nil, rows: [_ | _] = rows} = assigns} = term) do
    tail? = assigns.paging == :tail and assigns.request.opts[:before] == :end
    assign(term, selected: Rows.id(if tail?, do: List.last(rows), else: hd(rows)))
  end

  defp select_default(term), do: term

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

    rows = with_content_type(rows, page)
    if map_size(overview) == 0, do: rows, else: [overview(overview) | rows]
  end

  defp rows(%{object: nil}), do: []

  defp rows(%{object: object}),
    do: [Map.merge(object, %{decoded: {:ok, %{kind: :object}}, oversized_key: false})]

  defp rows(%{row: nil}), do: []
  defp rows(%{row: row}), do: [row]
  defp rows(page), do: [overview(page)]

  # Only the stream's metadata, which the page holds, has its content type.
  defp with_content_type(rows, %{
         metadata: %{row: %{decoded: {:ok, %{value: %{content_type: type}}}}}
       }),
       do: Enum.map(rows, &message_content_type(&1, type))

  defp with_content_type(rows, _page), do: rows

  defp message_content_type(%{decoded: {:ok, %{kind: :message}}} = row, type),
    do: Map.put(row, :content_type, type)

  defp message_content_type(row, _type), do: row

  defp overview(fields),
    do: %{key: <<>>, value: <<>>, decoded: {:ok, %{kind: :overview, value: fields}}}

  def refresh(%{assigns: %{preview: nil}} = term), do: {:noreply, start(term)}

  def refresh(term) do
    preview = term.assigns.preview
    row = Rows.selected(term.assigns)

    if whole?(row) do
      {:noreply,
       term |> start() |> assign(preview: preview, byte_history: term.assigns.byte_history)}
    else
      fetch_bytes(term, row, range(preview), term.assigns.byte_history)
    end
  end

  defp range(preview) do
    offset = preview[:offset] || 0
    if byte_size(preview.value) == 0, do: offset, else: {offset, byte_size(preview.value)}
  end

  defp whole?(row),
    do: is_map(row) and row.value_size == byte_size(row.value) and row[:truncated] != true

  defp confirm_or_read(term, request, capture) do
    if capture.encoded_bytes <= @confirm_above or
         term.assigns.approved == approval(request, capture),
       do: read_content(term, request, capture),
       else:
         assign(term,
           operation: :idle,
           status: "Confirm read",
           mode: beneath(term.assigns.mode, {:confirm, request, capture})
         )
  end

  @doc "Reads content after confirmation or when the input is below the confirmation threshold."
  def read_content(term, request, capture) do
    id = make_ref()
    submit(term, id, request)

    assign(term,
      id: id,
      request: request,
      operation: :reading,
      status: "Reading content",
      approved: approval(request, capture)
    )
  end

  # Root type choices do not change the input covered by a confirmation.
  defp approval(request, capture),
    do: {request.view, request.opts[:base], capture.owner, capture.encoded_bytes}

  # Log-content rows have no storage key, so their byte ranges must be sliced locally.
  def fetch_bytes(term, row, range, history) do
    if whole?(row),
      do: {:noreply, assign(term, preview: slice(row, range), byte_history: history)},
      else: read_bytes(term, row, range, history)
  end

  defp slice(row, range) do
    {offset, length} = if is_integer(range), do: {range, Request.range_bytes()}, else: range
    length = min(length, row.value_size - offset)
    row |> Map.put(:value, binary_part(row.value, offset, length)) |> Map.put(:offset, offset)
  end

  defp reslice(%{assigns: %{preview: %{} = preview}} = term) do
    row = Rows.selected(term.assigns)
    offset = preview[:offset] || 0

    cond do
      is_nil(row) or (offset > 0 and offset >= row.value_size) ->
        assign(term, preview: nil, byte_history: [])

      whole?(row) ->
        assign(term, preview: slice(row, range(preview)))

      true ->
        term
    end
  end

  defp reslice(term), do: term

  defp read_bytes(term, row, range, history) do
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
