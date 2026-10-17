defmodule Slap.Yjs.DocServer.Awareness do
  @moduledoc false

  alias Slap.Yjs
  alias Yex.DocServer.{State, Worker}

  @key Slap.Yjs.DocServer
  @stored :slap_yjs

  def decode_message(message) do
    case Yex.Sync.message_decode(message) do
      {:ok, {:awareness, update}} -> Yjs.Awareness.decode(update)
      {:ok, _message} -> :other
      {:error, _reason} -> {:error, :invalid}
    end
  end

  def request(message, pid, state) do
    with {:noreply, state} <- settle(state), do: handle_request(message, pid, state)
  end

  def relay(state, from, update) do
    with {:noreply, state} <- settle(state), do: apply_update(state, update, {@stored, from})
  end

  def depart(state, owner) do
    with {:noreply, state} <- settle(state) do
      {ids, owners} = Yjs.Owners.forget(persistence(state).owners, owner)
      state = update_owners(state, owners)
      unless ids == [], do: Yex.Awareness.remove_states(state.awareness, ids)
      settle(state)
    end
  end

  def group_changed(state, event, pids) do
    with {:noreply, state} <- settle(state),
         do: change_group(state, event, pids -- [self()])
  end

  def join_group(doc_id, pg) do
    case Process.whereis(pg) do
      nil ->
        nil

      _scope ->
        group = {@key, doc_id}
        {ref, _members} = :pg.monitor(pg, group)
        :ok = :pg.join(pg, group, self())
        ref
    end
  end

  def changed(awareness, change, origin, state) do
    %{added: added, updated: updated, removed: removed} = change
    p = persistence(state)

    state =
      update_owners(state, Yjs.Owners.changed(p.owners, owner(origin), added ++ updated, removed))

    case added ++ updated ++ removed do
      [] ->
        {:noreply, state}

      clients ->
        {:ok, update} = Yex.Awareness.encode_update(awareness, clients)

        for {pid, _ref} <- p.subscribers,
            pid != owner(origin),
            do: send(pid, {:slap_yjs_awareness, p.doc_id, update})

        unless match?({@stored, _}, origin), do: send_to_group(state, update)
        {:noreply, state}
    end
  end

  defp handle_request({:claim, id, principal}, pid, state) do
    p = persistence(state)

    result =
      if Map.has_key?(p.subscribers, pid),
        do: Yjs.Owners.claim(p.owners, id, pid, principal),
        else: {:error, :not_subscribed}

    case result do
      {:ok, owners} -> {:reply, :ok, update_owners(state, owners)}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp handle_request({:awareness, entries, pid}, pid, state) do
    owners = persistence(state).owners

    case Enum.filter(entries, fn {id, _clock, _state} -> Yjs.Owners.owns?(owners, id, pid) end) do
      [] ->
        {:reply, {:error, :not_owner}, state}

      entries ->
        with {:noreply, state} <- apply_update(state, Yjs.Awareness.encode(entries), pid),
             do: {:reply, :ok, state}
    end
  end

  defp handle_request({:awareness, _entries, _origin}, _pid, state),
    do: {:reply, {:error, :invalid_origin}, state}

  defp apply_update(state, update, origin) do
    :ok = Yex.Awareness.apply_update(state.awareness, update, origin)
    settle(state)
  end

  # y_ex queues callbacks even for synchronous awareness changes. Settle them
  # around lifecycle transitions so a delayed callback cannot undo a takeover.
  defp settle(state) do
    receive do
      {event, _change, _origin, Worker} = message
      when event in [:awareness_change, :awareness_update] ->
        with {:noreply, state} <- Worker.handle_info(message, state), do: settle(state)
    after
      0 -> {:noreply, state}
    end
  end

  defp change_group(state, _event, []), do: {:noreply, state}

  defp change_group(%State{awareness: awareness} = state, :join, pids) do
    # A peer removes what it learns here when this server leaves. Forwarding
    # states learned from another peer would give them the wrong cleanup owner.
    ids = Yex.Awareness.get_client_ids(awareness)

    with [_ | _] = clients <- Yjs.Owners.local(persistence(state).owners, ids),
         {:ok, update} <- Yex.Awareness.encode_update(awareness, clients) do
      for pid <- pids, do: send(pid, {@key, :awareness, self(), update})
    end

    {:noreply, state}
  end

  defp change_group(state, :leave, [pid | rest]) do
    with {:noreply, state} <- depart(state, {:server, pid}),
         do: change_group(state, :leave, rest)
  end

  defp owner({@stored, pid}), do: {:server, pid}
  defp owner(pid) when is_pid(pid), do: pid
  defp owner(_origin), do: nil

  defp send_to_group(state, update) do
    case persistence(state) do
      %{group: nil} ->
        :ok

      %{doc_id: doc_id, pg: pg} ->
        for pid <- :pg.get_members(pg, {@key, doc_id}),
            pid != self(),
            do: send(pid, {@key, :awareness, self(), update})

        :ok
    end
  end

  defp persistence(%State{assigns: assigns}), do: Map.fetch!(assigns, @key)

  defp update_owners(state, owners),
    do: State.assign(state, @key, %{persistence(state) | owners: owners})
end
