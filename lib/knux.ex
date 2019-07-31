defmodule Knux do
  @type mode :: :search | :ingest | :control

  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri), do: start_link(uri, [])
  def start_link(opts) when is_list(opts), do: Knux.Connection.start_link(validate!(opts))

  def start_link(uri, other_opts)

  def start_link(uri, other_opts) when is_binary(uri) and is_list(other_opts) do
    opts = Knux.Config.parse(uri)
    start_link(Keyword.merge(opts, other_opts))
  end

  def request(conn, request, opts \\ []) do
    Knux.Connection.request(conn, request, opts)
  end

  defp validate!(opts) do
    Knux.Config.validate!(opts)
  end
end
