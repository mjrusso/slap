defmodule Slap.Inspect.Options do
  @moduledoc false

  alias Slap.Inspect.Operation

  # Startup options excluded from command-editor queries.
  @connection [:node, :cookie_file, :cluster, :json, :longnames, :refresh]
  @switches [
    node: :string,
    cookie_file: :string,
    cluster: :string,
    json: :boolean,
    longnames: :boolean,
    shard: :integer,
    prefix: :string,
    key: :string,
    key_prefix: :string,
    partition: :string,
    path: :string,
    base: :string,
    component: :string,
    sid: :integer,
    expected_sid: :integer,
    from: :integer,
    until: :integer,
    limit: :integer,
    cursor: :string,
    cursor_exclusive: :boolean,
    offset: :integer,
    length: :integer,
    placement_key: :string,
    object_key: :string,
    instance: :string,
    budget: :integer,
    refresh: :integer,
    max_value_bytes: :integer,
    max_key_bytes: :integer,
    target_bytes: :integer,
    pins: :boolean,
    content: :boolean,
    root: :keep,
    at_offset: :integer,
    max_input_bytes: :integer,
    max_entries: :integer,
    max_output_bytes: :integer
  ]

  # An empty list marks an internal option with no CLI switch.
  @from_switches %{
    roots: [:root],
    value_range: [:offset, :length],
    preflight: [],
    retained: [],
    before: []
  }

  def switches, do: @switches
  def query_switches, do: Keyword.drop(@switches, @connection)
  def switch?(key), do: Keyword.has_key?(@switches, key)

  def query(opts), do: Keyword.drop(opts, @connection)

  def query_options, do: for({key, _type} <- query_switches(), do: option(key))

  def query_options(command) do
    command
    |> Operation.forms()
    |> Enum.flat_map(&accepted/1)
    |> Enum.uniq()
    |> Enum.map(&option/1)
  end

  def accepted(form) do
    (form.options ++ Map.get(form, :flags, []) ++ Operation.page(form) ++ [:shard, :budget])
    |> Enum.flat_map(&Map.get(@from_switches, &1, [&1]))
  end

  def option(key), do: "--" <> String.replace(Atom.to_string(key), "_", "-")

  @doc """
  Returns `:ok` if `form` accepts every query option in `opts`.
  Otherwise returns `{:error, {:unsupported_options, command, selectors, unused}}`.
  `selectors` contains the supplied flags and required options for this form.
  Each `unused` entry pairs an unsupported option with the additional options
  needed to accept it, or `nil` if no form of the command accepts it.
  """
  def check(opts, command, form) do
    given = for {key, _value} <- query(opts), uniq: true, do: key

    case given -- accepted(form) do
      [] ->
        :ok

      unused ->
        {:error,
         {:unsupported_options, command, Enum.filter(Operation.selectors(form), &(&1 in given)),
          Enum.map(unused, &{&1, missing(command, &1, given)})}}
    end
  end

  defp missing(command, key, given) do
    case for(
           form <- Operation.forms(command),
           key in accepted(form),
           do: Operation.selectors(form) -- given
         ) do
      [] -> nil
      candidates -> Enum.min_by(candidates, &length/1)
    end
  end
end
