defmodule Knux.Connection do
  use Connection

  defmodule Error do
    defexception [:function, :reason, :message]

    def exception({function, reason}) do
      message = "#{function} error: #{format_error(reason)}"
      %Error{function: function, reason: reason, message: message}
    end

    defp format_error(:closed), do: "closed"
    defp format_error(:timeout), do: "timeout"
    defp format_error(reason), do: :inet.format_error(reason)
  end

  defmodule State do
    @moduledoc false

    defstruct [
      # connection state: :init | :connected | :ready | {:wait_pending, from}
      # TODO describe states
      state: :init,

      # connection mode: :search | :ingest | :control
      # TODO describe modes
      mode: nil,

      # tcp socket
      socket: nil,

      # buffer?
      buffer: <<>>,

      # buffer continuation function from Knux.Proto.unpack/1
      buffer_cont: nil,

      # list of `GenServer.call(request)` requests, that waited for reply
      await: %{},

      # list of deferred requests, making while connection is busy
      deferred_requests: [],

      # options passed from start_link/1
      opts: [],
    ]
  end

  def start_link(opts \\ []) do
    Connection.start_link(__MODULE__, opts)
  end

  def request(conn, request, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)

    GenServer.call(conn, {:request, request}, timeout)
  end

  @impl true
  def init(opts) do
    {:connect, :info, %State{opts: opts}}
  end

  @impl true
  def connect(_info, %State{opts: opts} = state) do
    mode = Keyword.get(opts, :mode, :search)
    host = Keyword.fetch!(opts, :hostname) |> String.to_charlist()
    port = Keyword.fetch!(opts, :port)
    timeout = Keyword.get(opts, :timeout, 5_000)

    with {:ok, socket} <- :gen_tcp.connect(host, port, [:binary, active: false], timeout),
         :ok <- :inet.setopts(socket, [active: :once])
    do
      {:ok, %State{state | state: :init, mode: mode, socket: socket}}
    else
      {:error, reason} ->
        {:error, Error.exception({:connect, reason})}
    end
  end

  @impl true
  def disconnect(_reason, state) do
    {:backoff, 500, state}
  end

  @impl true
  def handle_call({:request, request}, from, %State{} = state) do
    case state.state do
      :ready -> {:noreply, make_request(request, from, state)}
      _other -> {:noreply, pend_request(request, from, state)}
    end
  end

  @impl true
  def handle_info({:tcp, _socket, data}, %State{} = state) do
    {responses, cont_fn} = unpack(data, state)
    state = %State{state | buffer_cont: cont_fn}

    state =
      Enum.reduce(responses, state, fn(response, state_acc) ->
        {:ok, parsed} = Knux.Proto.parse(response)
        handle_response(parsed, state_acc)
      end)

    case :inet.setopts(state.socket, active: :once) do
      :ok -> {:noreply, state}
      error -> {:disconnect, error, state}
    end
  end

  def handle_info({:tcp_closed, _socket}, state) do
    {:disconnect, :tcp_closed, state}
  end

  def handle_info(:set_mode, %State{} = state) do
    password = Keyword.get(state.opts, :password) || ""

    case send_request(["START", to_string(state.mode), password], state) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:error, reason}
    end
  end

  def handle_info(:process_deferred, %State{deferred_requests: []} = state) do
    {:noreply, state}
  end

  def handle_info(:process_deferred, %State{deferred_requests: [{from, request} | tail]} = state) do
    iodata = encode_request(request)
    send_request(iodata, state)

    {:noreply, %State{state | state: {:wait_pending, from}, deferred_requests: tail}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp unpack(data, %State{buffer_cont: cont}) when is_function(cont) do
    case cont.(data) do
      {:ok, response, rest} -> unpack_reduce(rest, {[response], nil})
      {:cont, cont_fn} -> {[], cont_fn}
    end
  end

  defp unpack(data, %State{buffer: buffer}) do
    unpack_reduce(<<buffer::binary, data::binary>>, {[], nil})
  end

  defp unpack_reduce(<<>>, {unpacked, cont_fn}) do
    {Enum.reverse(unpacked), cont_fn}
  end

  defp unpack_reduce(data, {already_unpacked, cont_fn_acc}) do
    case Knux.Proto.unpack(data) do
      {:ok, unpacked, rest} -> unpack_reduce(rest, {[unpacked | already_unpacked], cont_fn_acc})
      {:cont, cont_fn} -> {already_unpacked, cont_fn}
    end
  end

  defp handle_response(["CONNECTED" | _], %State{state: :init} = state) do
    send(self(), :set_mode)
    %State{state | state: :connected}
  end

  defp handle_response(["STARTED", type | _], %State{state: :connected} = state) when type in ["search", "ingest", "control"] do
    send(self(), :process_deferred)
    %State{state | state: :ready}
  end

  defp handle_response(["PENDING", id], %State{state: {:wait_pending, from}} = state) do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    %State{state | state: :ready, await: Map.put(state.await, id, from)}
  end

  defp handle_response(["EVENT", type, id | results], %State{} = state) when type in ["QUERY", "SUGGEST"] do
    case Map.pop(state.await, id, nil) do
      {nil, await} ->
        %State{state | await: await}

      {from, await} ->
        GenServer.reply(from, results)
        %State{state | await: await}
    end
  end

  defp handle_response(["OK"], %State{state: {:wait_pending, from}, mode: mode} = state) when mode in [:ingest, :control] do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    GenServer.reply(from, "OK")
    %State{state | state: :ready}
  end

  defp handle_response(["RESULT" | result], %State{state: {:wait_pending, from}} = state) do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    GenServer.reply(from, Enum.join(result, " "))
    %State{state | state: :ready}
  end

  defp handle_response(["PONG"], %State{state: {:wait_pending, from}} = state) do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    GenServer.reply(from, "PONG")
    %State{state | state: :ready}
  end

  defp handle_response(["ENDED", result], %State{state: {:wait_pending, from}} = state) do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    GenServer.reply(from, result)
    %State{state | state: :ready}
  end

  defp handle_response(["ERR", reason], %State{state: {:wait_pending, from}} = state) do
    if length(state.deferred_requests) > 0 do
      send(self(), :process_deferred)
    end

    GenServer.reply(from, {:error, reason})
    %State{state | state: :ready}
  end

  defp handle_response(_response, state) do
    state
  end

  defp send_request(query, %State{} = state) do
    :gen_tcp.send(state.socket, Knux.Proto.pack(query))
  end

  defp make_request(request, from, state) do
    iodata = encode_request(request)
    send_request(iodata, state)

    %State{state | state: {:wait_pending, from}}
  end

  defp pend_request(request, from, state) do
    %State{state | deferred_requests: [{from, request} | state.deferred_requests]}
  end

  defp encode_request(request) do
    Knux.Request.encode(request).request
  end
end
