defmodule Slap.Bench.RawHTTP do
  @moduledoc false
  # A minimal HTTP/1.1 client over one keep-alive :gen_tcp connection, for
  # the crash tests, the load benchmark and the server test: fast enough not
  # to be the bottleneck, and it reports a dropped connection as such.

  def connect(host \\ ~c"127.0.0.1", port) do
    :gen_tcp.connect(host, port, [:binary, active: false, packet: :raw, nodelay: true], 5_000)
  end

  def close(socket), do: :gen_tcp.close(socket)

  def request_once(port, method, path, headers \\ [], body \\ "") do
    case connect(port) do
      {:ok, socket} ->
        try do
          request(socket, method, path, headers, body)
        after
          close(socket)
        end

      {:error, _} = error ->
        error
    end
  end

  # Returns {:ok, status, headers (lowercase names), body} or {:error, reason}.
  def request(socket, method, path, headers \\ [], body \\ "") do
    head =
      [
        "#{method} #{path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: #{byte_size(body)}\r\n",
        Enum.map(headers, fn {k, v} -> "#{k}: #{v}\r\n" end),
        "\r\n"
      ]

    with :ok <- :gen_tcp.send(socket, [head, body]),
         {:ok, status, headers, rest} <- read_head(socket, ""),
         {:ok, body} <- read_body(socket, method, status, headers, rest) do
      {:ok, status, headers, body}
    end
  end

  defp read_head(socket, acc) do
    case :binary.split(acc, "\r\n\r\n") do
      [head, rest] ->
        [status_line | lines] = String.split(head, "\r\n")
        [_, code | _] = String.split(status_line, " ", parts: 3)

        headers =
          for line <- lines,
              [k, v] = String.split(line, ":", parts: 2),
              do: {String.downcase(k), String.trim(v)}

        {:ok, String.to_integer(code), headers, rest}

      [_] ->
        case :gen_tcp.recv(socket, 0, 60_000) do
          {:ok, data} -> read_head(socket, acc <> data)
          {:error, _} = error -> error
        end
    end
  end

  defp read_body(_socket, "HEAD", _status, _headers, _rest), do: {:ok, ""}

  defp read_body(_socket, _method, status, _headers, _rest) when status in [204, 304],
    do: {:ok, ""}

  defp read_body(socket, _method, _status, headers, rest) do
    cond do
      len = header(headers, "content-length") -> read_n(socket, String.to_integer(len), rest)
      header(headers, "transfer-encoding") == "chunked" -> read_chunked(socket, rest, [])
      true -> {:ok, rest}
    end
  end

  defp read_n(_socket, n, acc) when byte_size(acc) >= n, do: {:ok, binary_part(acc, 0, n)}

  defp read_n(socket, n, acc) do
    case :gen_tcp.recv(socket, 0, 60_000) do
      {:ok, data} -> read_n(socket, n, acc <> data)
      {:error, _} = error -> error
    end
  end

  defp read_chunked(socket, buf, acc) do
    case :binary.split(buf, "\r\n") do
      [size_line, rest] ->
        size = size_line |> String.split(";") |> hd() |> String.to_integer(16)

        if size == 0 do
          # The final CRLF (no trailers).
          with {:ok, _} <- fill(socket, rest, 2),
               do: {:ok, IO.iodata_to_binary(Enum.reverse(acc))}
        else
          with {:ok, chunk_and_more} <- fill(socket, rest, size + 2) do
            <<chunk::binary-size(^size), "\r\n", more::binary>> = chunk_and_more
            read_chunked(socket, more, [chunk | acc])
          end
        end

      [_] ->
        case :gen_tcp.recv(socket, 0, 60_000) do
          {:ok, data} -> read_chunked(socket, buf <> data, acc)
          {:error, _} = error -> error
        end
    end
  end

  defp fill(socket, acc, n) do
    if byte_size(acc) >= n do
      {:ok, acc}
    else
      case :gen_tcp.recv(socket, 0, 60_000) do
        {:ok, data} -> fill(socket, acc <> data, n)
        {:error, _} = error -> error
      end
    end
  end

  def header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {_, v} -> v
      nil -> nil
    end
  end
end
