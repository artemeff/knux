defmodule Knux.Config do
  @defaults [hostname: "localhost", port: 1491, password: nil, timeout: 15000]
  @integer_url_query_params ["timeout"]

  def parse(url) do
    Keyword.merge(@defaults, parse_url(url || ""))
  end

  def validate!(config) do
    validate_in!(config, :mode, :search, [:search, :ingest, :control])
    validate_in!(config, :log, false, [false, :debug, :info, :warn, :error])

    config
  end

  defp validate_in!(config, key, default, allowed) do
    value = Keyword.get(config, key, default)

    unless value in allowed do
      raise ArgumentError, "invalid #{inspect(key)} configuration, it can be " <>
                           "#{allowed |> Enum.map(&inspect/1) |> Enum.join(", ")}. got: #{inspect(value)}"
    end
  end

  defp parse_url(""), do: []

  defp parse_url(url) when is_binary(url) do
    info = URI.parse(url)

    if is_nil(info.host) do
      raise ArgumentError, url: url, message: "host is not present"
    end

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")

    url_opts = [
      username: username,
      password: password,
      hostname: info.host,
      port: info.port
    ]

    query_opts = parse_uri_query(info)

    for {k, v} <- url_opts ++ query_opts, not is_nil(v) do
      {k, if(is_binary(v), do: URI.decode(v), else: v)}
    end
  end

  defp parse_uri_query(%URI{query: nil}) do
    []
  end

  defp parse_uri_query(%URI{query: query} = url) do
    query
    |> URI.query_decoder()
    |> Enum.reduce([], fn
         {key, value}, acc when key in @integer_url_query_params ->
           [{String.to_atom(key), parse_integer!(key, value, url)}] ++ acc

         {key, value}, acc ->
           [{String.to_atom(key), value}] ++ acc
       end)
  end

  defp parse_integer!(key, value, url) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        raise ArgumentError, url: url, message: "can not parse value `#{value}` for parameter `#{key}` as an integer"
    end
  end
end
