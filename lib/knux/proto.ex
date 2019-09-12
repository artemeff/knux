defmodule Knux.Proto do
  # defstruct

  @crlf "\r\n"
  @crlf_iodata [?\r, ?\n]
  @args ~r/(\w+)\((.*)\)/

  @doc ~S"""
  Packs a list of Elixir terms to a Sonic query.

  ## Examples

      iex> iodata = Knux.Proto.pack(["START", "search", "SecretPassword"])
      iex> IO.iodata_to_binary(iodata)
      "START search SecretPassword\r\n"

      iex> iodata = Knux.Proto.pack(["QUERY", "messages", "user:0dcde3a6", {:quoted, "valerian saliou"}, {"LIMIT", 10}])
      iex> IO.iodata_to_binary(iodata)
      "QUERY messages user:0dcde3a6 \"valerian saliou\" LIMIT(10)\r\n"

  """
  @spec pack(%Knux.Request{} | [binary | {:quoted, binary} | {binary, binary | integer}]) :: iodata

  def pack(%Knux.Request{io_data: io_data}) do
    pack(io_data)
  end

  def pack(items) when is_list(items) do
    pack(items, [])
  end

  defp pack([item | []], acc) do
    pack([], [acc, pack_item(item)])
  end

  defp pack([item | rest], acc) do
    pack(rest, [acc, pack_item(item), ?\s])
  end

  defp pack([], acc) do
    [acc, @crlf_iodata]
  end

  defp pack_item({:quoted, binary}) do
    [?", binary, ?"]
  end

  defp pack_item({name, binary}) do
    [name, ?(, to_string(binary), ?)]
  end

  defp pack_item(binary) do
    binary
  end

  @doc ~S"""
  Unpacks Sonic query.

  ## Examples

      iex> Knux.Proto.unpack("START search SecretPassword\r\n")
      {:ok, "START search SecretPassword", ""}

      iex> Knux.Proto.unpack("START search SecretPassword\r\nCOMMAND test\r\nCOMMAND")
      {:ok, "START search SecretPassword", "COMMAND test\r\nCOMMAND"}

      iex> {:cont, cont_fn} = Knux.Proto.unpack("START search")
      iex> {:cont, cont_fn} = cont_fn.(" SecretPassword")
      iex> cont_fn.("123\r\nrest")
      {:ok, "START search SecretPassword123", "rest"}

      iex> {:cont, cont_fn} = Knux.Proto.unpack("START search\r")
      iex> cont_fn.("\n")
      {:ok, "START search", ""}

      iex> Knux.Proto.unpack("QUERY messages user:0dcde3a6 \"valerian saliou\" LIMIT(10)\r\n")
      {:ok, "QUERY messages user:0dcde3a6 \"valerian saliou\" LIMIT(10)", ""}

  """
  @spec unpack(binary) :: iodata
  def unpack(binary) do
    unpack(binary, "")
  end

  defp unpack(<<@crlf, rest::binary>>, acc) do
    {:ok, acc, rest}
  end

  defp unpack(<<>>, acc) do
    {:cont, &unpack(&1, acc)}
  end

  defp unpack(<<?\r>>, acc) do
    {:cont, &unpack(<<?\r, &1::binary>>, acc)}
  end

  defp unpack(<<byte, rest::binary>>, acc) do
    unpack(rest, <<acc::binary, byte>>)
  end

  @doc ~S"""
  Parses unpacked Sonic query.

  ## Examples

      iex> Knux.Proto.parse("CONNECTED <sonic-server v1.0.0>")
      {:ok, ["CONNECTED", "<sonic-server v1.0.0>"]}

      iex> Knux.Proto.parse("STARTED search protocol(1) buffer(20000)")
      {:ok, ["STARTED", "search", {"protocol", "1"}, {"buffer", "20000"}]}

      iex> Knux.Proto.parse("EVENT QUERY Bt2m2gYa conversation:71f3d63b conversation:6501e83a")
      {:ok, ["EVENT", "QUERY", "Bt2m2gYa", "conversation:71f3d63b", "conversation:6501e83a"]}

  """
  @spec parse(binary) :: list(term)
  def parse(<<"CONNECTED ", rest::binary>>) do
    {:ok, ["CONNECTED", rest]}
  end

  def parse(<<"STARTED ", rest::binary>>) do
    [mode | arguments] = String.split(rest)
    {:ok, ["STARTED", mode | parse_mode_args(arguments)]}
  end

  def parse(binary) do
    {:ok, String.split(binary, " ")}
  end

  defp parse_mode_args(args) do
    Enum.map(args, fn(arg) ->
      case Regex.run(@args, arg) do
        [_, name, param] -> {name, param}
        _ -> arg
      end
    end)
  end
end
