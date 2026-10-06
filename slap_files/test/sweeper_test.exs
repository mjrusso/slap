defmodule Slap.Files.SweeperTest do
  # The states a writer leaves when it stops between two steps, built from
  # the intents, records and objects it would have written, and what the
  # sweeper makes of each: every object that is not a file's body is
  # deleted once its intent is due, and no body is.
  use Slap.Files.Test.FilesCase, async: false

  alias Slap.Files.{Config, Intent, Object, Record, Sweeper}
  alias Slap.SlateDB.ObjectStore

  @upload_timeout 10_000
  @retention 1_000

  defp object!(key) do
    {:ok, _} = ObjectStore.put(Config.get().objects, key, "body of #{key}")
    key
  end

  defp record(key),
    do: %{body: {:object, key}, size: 1, sha256: "", content_type: "x", metadata: %{}}

  test "an upload that stopped before its file pointed to it is deleted when due" do
    key = upload!({"p", "new"})

    sweep()
    assert objects() == [key]

    advance(@upload_timeout)
    sweep()
    assert objects() == []
    assert intents() == []
  end

  test "synchronous cleanup reports object store failures" do
    upload!({"p", "failed-cleanup"})
    advance(@upload_timeout)

    {:ok, broken} =
      ObjectStore.open(
        "files",
        store:
          {:url, "s3://unreachable",
           aws_endpoint: "http://127.0.0.1:1",
           aws_allow_http: true,
           aws_region: "us-east-1",
           aws_access_key_id: "x",
           aws_secret_access_key: "x"}
      )

    bad_name = __MODULE__.BrokenSweeper
    config = %{Config.get() | name: bad_name, objects: broken, timeout: 1}
    start_supervised!({Sweeper, config: config})

    assert {:error, _} = Sweeper.sweep(bad_name)
    assert {:error, _} = Sweeper.reconcile(bad_name)
  end

  test "an object that an upload writes after it was deleted is deleted by reconciliation" do
    key = upload!({"p", "late"})
    advance(@upload_timeout)
    sweep()

    # A store request completes after the sweep.
    object!(key)
    sweep()
    assert objects() == [key]

    reconcile()
    assert objects() == []
  end

  test "reconciliation keeps files' bodies and uploads in progress" do
    {:ok, _} = Files.put({"p", "file"}, String.duplicate("f", 100))
    body = object_key({"p", "file"})
    uploading = upload!({"p", "uploading"})

    reconcile()
    assert objects() == Enum.sort([body, uploading])
  end

  test "a replace that stopped after taking its intent keeps the body the file points to" do
    ref = {"p", "rep"}

    [old, new] = [
      object!(Object.new_key(ref, Config.get())),
      object!(Object.new_key(ref, Config.get()))
    ]

    {:ok, _} = Record.put(ref, record(old), :absent, nil, Config.get())

    {:ok, intent} =
      Intent.open(ref, [new], Config.now(Config.get()) + @upload_timeout, Config.get())

    {:ok, _} =
      Intent.take(intent,
        keys: [new, old],
        due_ms: Config.now(Config.get()) + @retention,
        taken: :writer
      )

    advance(@retention)
    sweep()
    assert objects() == [old]
    assert intents() == []
  end

  test "a replace that stopped after switching the record deletes only the old body" do
    ref = {"p", "sw"}

    [old, new] = [
      object!(Object.new_key(ref, Config.get())),
      object!(Object.new_key(ref, Config.get()))
    ]

    {:ok, v} = Record.put(ref, record(old), :absent, nil, Config.get())

    {:ok, intent} =
      Intent.open(ref, [new], Config.now(Config.get()) + @upload_timeout, Config.get())

    {:ok, _} =
      Intent.take(intent,
        keys: [new, old],
        due_ms: Config.now(Config.get()) + @retention
      )

    {:ok, _} = Record.put(ref, record(new), v, nil, Config.get())

    advance(@retention)
    sweep()
    assert objects() == [new]
  end

  test "a delete that stopped before deleting the record keeps the body" do
    ref = {"p", "del"}
    key = object!(Object.new_key(ref, Config.get()))
    {:ok, _} = Record.put(ref, record(key), :absent, nil, Config.get())
    {:ok, _} = Intent.open(ref, [key], Config.now(Config.get()) + @retention, Config.get())

    advance(@retention)
    sweep()
    assert objects() == [key]
    assert {:ok, {_, _}} = Record.get(ref, [], Config.get())
    assert intents() == []
  end

  test "a sweep that stopped after taking an intent is finished by the next" do
    ref = {"p", "half"}

    {:ok, intent} =
      Intent.open(
        ref,
        [object!(Object.new_key(ref, Config.get()))],
        Config.now(Config.get()) + @upload_timeout,
        Config.get()
      )

    {:ok, _} = Intent.take(intent, taken: :sweeper)

    advance(@upload_timeout)
    sweep()
    assert objects() == []
    assert intents() == []
  end

  test "an upload slower than upload_timeout_ms fails, and leaves nothing" do
    body = held_body(self())
    put = Task.async(fn -> Files.put({"p", "slow"}, body) end)
    assert_receive {:uploading, uploader}, 5_000

    # The body is read, and the upload has yet to write the object: the
    # sweep deletes the object first.
    advance(@upload_timeout)
    sweep()
    assert intents() == []

    send(uploader, :go)
    assert {:error, :expired} = Task.await(put)
    assert {:ok, nil} = Files.get({"p", "slow"})
    assert [_] = objects()

    reconcile()
    assert objects() == []
  end

  test "a record write that waited past its intent's due time keeps the body it points to" do
    ref = ref_with_own(:record)
    {:ok, _} = Files.put(ref, String.duplicate("a", 100))
    old = object_key(ref)

    put = held_write(ref, :record, fn -> Files.put(ref, String.duplicate("b", 100)) end)

    # The intent that names both bodies is due, and the record write it
    # covers has not been handled yet. The sweeper waits for it.
    advance(@retention)
    sweeper = Task.async(&sweep/0)
    assert Task.yield(sweeper, 100) == nil
    :ok = :sys.resume(writer(ref, :record))

    assert {:ok, _} = Task.await(put)
    assert :ok = Task.await(sweeper)
    new = object_key(ref)
    assert new != old
    assert objects() == [new]
    assert {:ok, body} = Files.read(ref)
    assert body == String.duplicate("b", 100)
  end

  @tag files: [retention_ms: 20]
  test "a record write handled after its intent is due is not applied" do
    ref = ref_with_own(:record)
    {:ok, _} = Files.put(ref, String.duplicate("a", 100))
    old = object_key(ref)

    put = held_write(ref, :record, fn -> Files.put(ref, String.duplicate("b", 100)) end)

    # Slap.KV checks the deadline (the intent's due time) by the system
    # clock.
    Process.sleep(30)
    :ok = :sys.resume(writer(ref, :record))
    assert {:error, :timeout} = Task.await(put)
    assert object_key(ref) == old

    advance(20)
    sweep()
    assert objects() == [old]
    assert intents() == []
  end

  @tag files: [upload_timeout_ms: 20]
  test "a registration handled after its intent is due is not applied" do
    ref = ref_with_own(:registration)
    put = held_write(ref, :registration, fn -> Files.put(ref, String.duplicate("b", 100)) end)

    Process.sleep(30)
    :ok = :sys.resume(writer(ref, :registration))
    assert {:error, :timeout} = Task.await(put)
    assert registrations() == []

    # The intent is left due when the registration could no longer be
    # applied.
    assert [_] = intents()
    advance(20)
    sweep()
    assert intents() == []
    assert objects() == []
  end

  @tag files: [upload_timeout_ms: 20]
  test "an intent that would be opened after it is due is not" do
    ref = ref_with_own(:intent)
    put = held_write(ref, :intent, fn -> Files.put(ref, String.duplicate("b", 100)) end)

    Process.sleep(30)
    :ok = :sys.resume(writer(ref, :intent))
    assert {:error, :timeout} = Task.await(put)
    assert intents() == []
    assert registrations() == []
    assert objects() == []
  end

  @tag files: [max_clock_skew_ms: 500]
  test "an intent is acted on max_clock_skew_ms after it is due" do
    ref = {"p", "skew"}
    key = object!(Object.new_key(ref, Config.get()))
    {:ok, _} = Intent.open(ref, [key], Config.now(Config.get()) + @upload_timeout, Config.get())

    sweep()
    assert objects() == [key]

    advance(@upload_timeout + 499)
    sweep()
    assert objects() == [key]

    advance(1)
    sweep()
    assert objects() == []
    assert intents() == []
  end

  # 20 bytes (more than inline_max_bytes: an object), then a wait before
  # the end: the uploading process sends `{:uploading, pid}` to `test`, and
  # ends the body once it gets `:go`.
  defp held_body(test) do
    Stream.resource(
      fn -> :first end,
      fn
        :first ->
          {[String.duplicate("x", 20)], :wait}

        :wait ->
          send(test, {:uploading, self()})

          receive do
            :go -> {:halt, :done}
          end
      end,
      fn _ -> :ok end
    )
  end

  # The state an upload of a body to `ref` is in once it has written its
  # object: its key is named by an intent, and registered.
  defp upload!(ref) do
    key = Object.new_key(ref, Config.get())
    {:ok, _} = Intent.open(ref, [key], Config.now(Config.get()) + @upload_timeout, Config.get())
    :ok = Object.register(key, Config.now(Config.get()) + @upload_timeout, Config.get())
    object!(key)
  end

  # A ref whose `kind` of row (:record, :registration or :intent) has a
  # partition writer that none of its other rows has, so that holding it up
  # holds up nothing else of the ref's.
  defp ref_with_own(kind) do
    Stream.map(1..1_000, &{"p", "held#{&1}"})
    |> Enum.find(fn ref ->
      others = for {k, partition} <- partitions(ref), k != kind, do: partition_writer(partition)
      writer(ref, kind) not in others
    end)
  end

  defp partitions({partition, _id} = ref) do
    bucket = Intent.bucket(ref)

    %{
      record: Config.partition(Config.get(), :record, partition),
      registration: Object.partition(bucket, Config.get()),
      intent: Intent.partition(bucket, Config.get())
    }
  end

  defp writer(ref, kind), do: partition_writer(partitions(ref)[kind])

  # Runs `fun` in a task, and returns once its write of `ref`'s `kind` of
  # row is waiting for the row's suspended partition writer.
  defp held_write(ref, kind, fun) do
    writer = writer(ref, kind)
    :ok = :sys.suspend(writer)
    1 = :erlang.trace(writer, true, [:receive])
    task = Task.async(fun)
    assert_receive {:trace, ^writer, :receive, {:"$gen_call", _, {:write, {:put, _, _}, _, _}}}
    1 = :erlang.trace(writer, false, [:receive])
    task
  end
end
