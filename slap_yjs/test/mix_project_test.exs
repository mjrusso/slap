defmodule Slap.Yjs.MixProjectTest do
  use ExUnit.Case, async: true

  @tag :tmp_dir
  test "loads the project in a bundled runtime without OTP_VERSION", %{tmp_dir: root} do
    on_exit(fn -> File.rm_rf!(root) end)

    for dir <- ["bin", "lib", "erts-#{:erlang.system_info(:version)}"] do
      File.ln_s!(Path.join(to_string(:code.root_dir()), dir), Path.join(root, dir))
    end

    project = Path.expand("../mix.exs", __DIR__)

    script = """
    Mix.start()
    Code.compile_file(#{inspect(project)})
    IO.puts(Mix.Project.config()[:app])
    """

    {output, status} =
      System.cmd(System.find_executable("elixir"), ["-e", script],
        env: [{"ERL_ROOTDIR", root}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "slap_yjs"
  end
end
