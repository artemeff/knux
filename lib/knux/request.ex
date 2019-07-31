defmodule Knux.Request do
  use Knux.Request.Macro

  defstruct [:mode, :request]

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

  def encode(%{__meta__: meta} = struct) do
    %__MODULE__{
      mode: meta.mode,
      request: [
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
end
