defmodule Slap.Files.S3Test do
  # Ranges of object bodies on S3, where each is a ranged GET. The larger
  # body is a multipart upload.
  use Slap.Files.Test.FilesCase, async: false

  @moduletag :s3

  test "ranged reads of object bodies" do
    for size <- [1_000, 6 * 1024 * 1024 + 7] do
      ref = {"s3-ranges", "#{size}"}
      body = :crypto.strong_rand_bytes(size)
      assert {:ok, %{storage: :object}} = Files.put(ref, body)

      for {range, {first, last} = resolved} <- [
            {{0, 0}, {0, 0}},
            {{1, size - 2}, {1, size - 2}},
            {{size - 10, size + 1_000}, {size - 10, size - 1}},
            {{size - 1, :eof}, {size - 1, size - 1}},
            {{:last, 20}, {size - 20, size - 1}},
            {{:last, size * 2}, {0, size - 1}}
          ] do
        assert {:ok, {_file, ^resolved, chunks}} = Files.stream(ref, range: range)

        assert IO.iodata_to_binary(Enum.to_list(chunks)) ==
                 binary_part(body, first, last - first + 1)
      end
    end
  end
end
