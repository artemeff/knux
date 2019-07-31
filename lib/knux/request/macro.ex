defmodule Knux.Request.Macro do
  defmacro __using__(_opts \\ []) do
    quote do
      require Knux.Request.Macro
      import  Knux.Request.Macro
    end
  end

  defmacro defr(mode, name, args \\ [], opts \\ []) do
    string = name |> to_string() |> String.upcase()
    module = Module.concat(__CALLER__.module, String.to_atom(Macro.camelize(to_string(name))))

    args_struct = Enum.map(args, &arg_to_struct/1)
    args_meta = Enum.zip(args_struct, args)

    enforce = Enum.reduce(args, [], fn
      ({:optional, _, _}, acc) -> acc
      ({_k, _v}, acc) -> acc
      (k, acc) -> acc ++ [arg_to_struct(k)]
    end)

    opts_meta = Enum.map(opts, fn(opt) ->
      {opt, opt |> to_string() |> String.upcase()}
    end)

    quote do
      defmodule unquote(module) do
        @moduledoc false
        @enforce_keys unquote(enforce)
        defstruct unquote(args_struct) ++ unquote(opts) ++ [
          __meta__: %{
            mode: unquote(mode),
            name: unquote(string),
            args: unquote(args_meta),
            opts: unquote(opts_meta),
          }
        ]
      end
    end
  end

  def quoted(key) do
    {:"$", :quoted, key}
  end

  def optional(key) do
    {:"$", :optional, key}
  end

  def arg_key(key) do
    key
  end

  defp arg_to_struct({_, [line: _], [key]}) do
    key
  end

  defp arg_to_struct(key) do
    key
  end
end
