defmodule Knux.Request do
  use Knux.Request.Macro

  defstruct [:mode, :io_data]

  # search

  defr :search, :query, [:collection, :bucket, quoted(:query)], [:limit, :offset, :lang]

  defr :search, :suggest, [:collection, :bucket, quoted(:word)], [:limit]

  # ingest

  defr :ingest, :push, [:collection, :bucket, :object, quoted(:text)], [:lang]

  defr :ingest, :pop, [:collection, :bucket, :object, quoted(:text)]

  defr :ingest, :count, [:collection, optional(:bucket), optional(:object)]

  defr :ingest, :flushc, [:collection]

  defr :ingest, :flushb, [:collection, :bucket]

  defr :ingest, :flusho, [:collection, :bucket, :object]

  # control

  defr :control, :trigger, [optional(:action), optional(:data)]

  defr :control, :info

  # generic

  defr :any, :ping

  defr :any, :help, [optional(:manual)]

  defr :any, :quit

  # encoding

  @doc """
  Encodes Knux.Request into iolist
  """
  def encode(%{__meta__: meta} = struct) do
    %__MODULE__{
      mode: meta.mode,
      io_data: [
        meta.name |
          Enum.reduce(meta.args, [], &(encode_arg(struct, &1, &2))) ++
          Enum.reduce(meta.opts, [], &(encode_opt(struct, &1, &2)))
      ],
    }
  end

  defp encode_arg(struct, {key, type}, acc) do
    case Map.fetch!(struct, key) do
      nil -> acc
      val -> acc ++ [encode_arg_type(val, type)]
    end
  end

  defp encode_arg_type(val, {:"$", :optional, _}) do
    val
  end

  defp encode_arg_type(val, {:"$", type, _}) do
    {type, val}
  end

  defp encode_arg_type(val, _) do
    val
  end

  defp encode_opt(struct, {key, opt_name}, acc) do
    case Map.fetch!(struct, key) do
      nil -> acc
      val -> acc ++ [{opt_name, val}]
    end
  end

  @doc """
  Chunks Knux.Request into smaller parts by provided `io_size_overflow`,
  that logic should be inside `Knux.Connection` in the future.
  """
  def chunk(%{__meta__: %{args_quoted: quoted_arg}} = struct, io_size_overflow) when not is_nil(quoted_arg) do
    overflow = round(io_size_overflow * 0.7)
    quoted = Map.fetch!(struct, quoted_arg)

    if io_size(quoted) >= io_size_overflow do
      chunks = split_words(IO.iodata_to_binary(quoted), overflow)

      Enum.reduce(chunks, [], fn(chunk, acc) ->
        [Map.put(struct, quoted_arg, chunk) | acc]
      end)
    else
      [struct]
    end
  end

  def chunk(%{__meta__: _} = struct, _io_size_overflow) do
    [struct]
  end

  defp io_size(io) do
    :erlang.iolist_size(io)
  end

  defp split_words(string, io_size_overflow) do
    [word | rest] = :binary.split(string, [" ", ".", "-", ",", ";", "\n", "\r"], [:global, :trim])
    split_words(rest, io_size_overflow, byte_size(word), word, [])
  end

  defp split_words([], _, _, line, acc) do
    [line | acc]
  end

  defp split_words(["" | rest], max, line_length, line, acc) do
    split_words(rest, max, line_length, line, acc)
  end

  defp split_words([word | rest], max, line_length, line, acc) do
    size = byte_size(word)

    if line_length + 1 + size > max do
      split_words(rest, max, size, word, [line | acc])
    else
      split_words(rest, max, line_length + 1 + size, line <> " " <> word, acc)
    end
  end
end
