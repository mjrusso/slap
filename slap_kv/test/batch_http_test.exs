defmodule Slap.KV.BatchHTTPTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Plug.Conn
  import Plug.Test
  import Slap.KV.Test.WriterControl

  alias Slap.KV.HTTP.Router

  @moduletag :capture_log

  test "mutations return their version and ETag; check-only responses return a null version without ETag" do
    response =
      post(%{
        ops: [
          %{op: "put", key: b64("a"), value: b64("1"), if_absent: true},
          %{op: "put", key: b64("b"), value: b64("2")}
        ]
      })

    assert response.status == 200
    assert %{"version" => version} = JSON.decode!(response.resp_body)
    assert get_resp_header(response, "etag") == [~s("#{version}")]
    assert get_resp_header(response, "content-type") == ["application/json; charset=utf-8"]
    check = post(%{ops: [%{op: "check", key: b64("a"), if_version: version}]})
    assert check.status == 200
    assert JSON.decode!(check.resp_body) == %{"version" => nil}
    assert get_resp_header(check, "etag") == []
    conflict = post(%{ops: [%{op: "put", key: b64("a"), value: "", if_absent: true}]})
    assert conflict.status == 412
    assert JSON.decode!(conflict.resp_body) == %{"key" => b64("a"), "version" => version}
    missing = post(%{ops: [%{op: "check", key: b64("missing"), if_version: 0}]})
    assert JSON.decode!(missing.resp_body) == %{"key" => b64("missing"), "version" => nil}
  end

  test "conflicts return arbitrary binary keys as base64url" do
    key = <<0, 255, ?/, 128>>
    {:ok, version} = KV.put("p", key, <<255, 0>>)
    response = post(%{ops: [%{op: "check", key: b64(key), if_absent: true}]})
    assert response.status == 412
    assert JSON.decode!(response.resp_body) == %{"key" => b64(key), "version" => version}
  end

  test "chunked bodies span bounded reads and respect the raw JSON cap" do
    value = :binary.copy(<<255>>, 6 * 1024 * 1024)
    body = JSON.encode!(%{ops: [%{op: "put", key: b64("large"), value: b64(value)}]})
    headers = [{"transfer-encoding", "chunked"}]
    assert raw(body, [max_batch_body: byte_size(body) - 1], headers).status == 413
    assert {:ok, nil} = KV.get("p", "large")
    assert raw(body, [], headers).status == 200
    assert {:ok, %{value: ^value}} = KV.get("p", "large")
  end

  test "strict JSON shapes, conditions and canonical unpadded base64url" do
    bad = [
      nil,
      [],
      %{},
      %{ops: "bad"},
      %{ops: [], other: 1},
      %{ops: [], deadline: nil},
      %{ops: [], deadline: 1.5}
    ]

    for body <- bad, do: assert(post(body).status == 400)

    ops = [
      %{op: "put", key: "YQ", value: "MQ", unknown: 1},
      %{op: "put", key: "YQ", value: 1},
      %{op: "put", key: "YQ", value: "MQ", if_version: 1, if_absent: true},
      %{op: "put", key: "YQ", value: "MQ", if_absent: false},
      %{op: "put", key: "YQ", value: "MQ", if_version: nil},
      %{op: "delete", key: "YQ", if_absent: true},
      %{op: "delete", key: "YQ", value: ""},
      %{op: "check", key: "YQ"},
      %{op: "check", key: "YQ", if_absent: true, value: ""},
      %{op: "check", key: "YQ==", if_absent: true},
      %{op: "check", key: "YR", if_absent: true},
      %{op: "check", key: "+/", if_absent: true},
      %{op: "check", key: "!", if_absent: true},
      %{op: "check", key: nil, if_absent: true},
      %{op: "check", key: "YQ", if_version: -1}
    ]

    for op <- ops, do: assert(post(%{ops: [op]}).status == 400, inspect(op))
    assert raw("{").status == 400
    assert {:ok, %{rows: []}} = KV.scan("p")
  end

  test "media type, single-row conditions, body/value limits and deadlines" do
    assert raw("{}", [], [{"content-type", "text/plain"}]).status == 415

    for header <- ["if-match", "if-none-match"],
        do: assert(raw("{}", [], [{header, "*"}]).status == 400)

    body = %{ops: [%{op: "put", key: b64("k"), value: b64("1234")}]}
    assert post(body, max_value: 3).status == 413
    assert post(body, max_batch_body: byte_size(JSON.encode!(body)) - 1).status == 413
    assert post(body, max_batch_body: byte_size(JSON.encode!(body))).status == 200
    response = post(%{ops: [%{op: "delete", key: b64("k")}], deadline: 0})
    assert response.status == 400
    assert response.resp_body =~ "deadline_exceeded"
    assert get_resp_header(response, "retry-after") == []
    assert_raise ArgumentError, fn -> Router.init(max_batch_body: 0) end
  end

  @tag child_options: [max_batch_ops: 1, max_inflight_bytes_per_writer: 6]
  test "owner rejects decoded batch and request sizes independently of HTTP limits" do
    response = post(%{ops: [%{op: "delete", key: b64("a")}, %{op: "delete", key: b64("b")}]})
    assert response.status == 413
    assert response.resp_body =~ "batch_too_large"
    response = post(%{ops: [%{op: "put", key: b64("a"), value: b64("123")}]})
    assert response.status == 413
    assert response.resp_body =~ "request_too_large"
    assert get_resp_header(response, "retry-after") == []
  end

  @tag settings: %{flush_interval: nil}
  @tag child_options: [max_inflight_requests_per_writer: 1]
  test "overloaded batches return a definite rejection with a retry header" do
    task = pending("p", fn -> KV.put("p", "k", "v") end)
    response = post(%{ops: [%{op: "delete", key: b64("a")}]})
    assert response.status == 503
    assert response.resp_body == "overloaded: request was not applied"
    assert get_resp_header(response, "retry-after") == ["1"]
    flush("p")
    assert {:ok, _} = Task.await(task)
  end

  defp post(body, opts \\ []), do: raw(JSON.encode!(body), opts)

  defp raw(body, opts \\ [], headers \\ []) do
    conn(:post, "/v1/kv/p", body)
    |> put_req_header("content-type", "application/json")
    |> then(fn conn ->
      Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    end)
    |> Router.call(Router.init(opts))
  end

  defp b64(value), do: Base.url_encode64(value, padding: false)
end
