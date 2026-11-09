defmodule Slap.Inspect.ViewCase do
  @moduledoc false

  # Drives Slap.Inspect.View through Breeze.Test. The view's reads go to the
  # test: each sends `{:read, request, worker}`, with the cluster read under
  # the request's `:cluster` key, and waits for the test to answer.
  # `answer/2` and `reply/3` deliver results through the real request worker
  # and return once the result is in the view's mailbox, so a render that
  # follows sees it.

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Breeze.Test
  alias Slap.Inspect.{Request, View}

  def start_view(opts \\ []) do
    client = self()

    read = fn connection, request ->
      send(client, {:read, Map.put(request, :cluster, connection.cluster), self()})
      receive do: ({:reply, result} -> result)
    end

    {:ok, request} =
      %{view: :raw, shard: 0, budget: 5000, opts: []}
      |> Map.merge(Map.new(opts[:request] || []))
      |> Request.new()

    default = %{cluster: Slap.Streams.Cluster, services: [:streams, :kv]}
    clusters = Keyword.get(opts, :clusters, [default])

    session =
      Test.start!(View,
        size: Keyword.get(opts, :size, {80, 24}),
        start_opts: [
          connection: %{entry: :slap@localhost, cluster: hd(clusters).cluster},
          clusters: clusters,
          request: request,
          read: read,
          refresh: Keyword.get(opts, :refresh, 0)
        ]
      )

    on_exit(fn -> Test.stop(session) end)
    session
  end

  @doc "Waits for the view's next read, answers it, and returns its request."
  def answer(session, result) do
    assert_receive {:read, request, worker}
    reply(session, worker, result)
    request
  end

  def reply(session, worker, result) do
    pid = session.pid
    :erlang.trace(pid, true, [:receive])
    send(worker, {:reply, result})
    assert_receive {:trace, ^pid, :receive, {:inspection_result, _id, ^result}}, 1000
    :erlang.trace(pid, false, [:receive])
    :ok
  end

  # Stands in for the refresh timer, which fires after the configured interval.
  def fire_refresh(session) do
    %{assigns: %{id: id}} = Test.metadata(session)
    Test.info(session, {:refresh, id})
  end

  def edit(session, command) do
    Test.input(session, "/")
    Test.input(session, "\x15")
    # The editor takes focus when the screen next renders, as in a terminal.
    Test.render!(session)
    type(session, command)
  end

  @doc "Presses d until the details pane shows `mode`."
  def show(session, mode, presses \\ 4) do
    cond do
      Test.render!(session) =~ "Details · #{mode}" ->
        :ok

      presses == 0 ->
        flunk("details never showed #{mode}")

      true ->
        Test.input(session, "d")
        show(session, mode, presses - 1)
    end
  end

  def type(session, text), do: Enum.each(String.graphemes(text), &Test.input(session, &1))

  def lines(screen), do: String.split(screen, "\n")

  @doc "The label of the highlighted record."
  def selected_label(screen) do
    [_match, label] = Regex.run(~r/\e\[48;5;6;38;5;0m([^\e]*)\e\[0m/, screen)
    String.trim(label)
  end

  def row,
    do: %{
      key: "a",
      value: <<0, 255, 27>>,
      oversized_key: false,
      value_size: 3,
      decoded: {:ok, %{kind: :raw}}
    }

  def page, do: %{owner: :slap@localhost, shard: 0, durable_seq: 4, cursor: "b", rows: [row()]}
end
