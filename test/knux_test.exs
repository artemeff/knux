defmodule KnuxTest do
  use ExUnit.Case
  doctest Knux

  @uri "tcp://:password@localhost:1491?timeout=1000"

  describe "#start_link" do
    test "returns pid" do
      assert {:ok, _pid} = Knux.start_link(@uri)
    end

    test "raise error with invalid mode" do
      assert_raise ArgumentError, "invalid :mode configuration, it can be :search, :ingest, :control. got: :wrong", fn ->
        Knux.start_link(@uri, mode: :wrong)
      end
    end

    test "raise error with invalid log level" do
      assert_raise ArgumentError, "invalid :log configuration, it can be false, :debug, :info, :warn, :error. got: :wrong", fn ->
        Knux.start_link(@uri, log: :wrong)
      end
    end
  end

  describe "mode = :search" do
    setup [:setup_cleanup, :setup_populate, :setup_consolidate]

    setup do
      {:ok, conn} = Knux.start_link(@uri, mode: :search)
      {:ok, conn: conn}
    end

    test "#query", %{conn: conn} do
      assert ["some:1"] = Knux.request(conn, %Knux.Request.Query{collection: "knux", bucket: "default", query: "query", limit: 42})
    end

    test "#query async", %{conn: conn} do
      assert [{1, ["some:1"]}, {2, ["some:1"]}, {3, ["some:1"]}] ==
        request_async(conn, 3, fn(_) ->
          %Knux.Request.Query{collection: "knux", bucket: "default", query: "query", limit: 42}
        end)
    end

    test "#suggest", %{conn: conn} do
      assert ["query"] = Knux.request(conn, %Knux.Request.Suggest{collection: "knux", bucket: "default", word: "query"})
    end

    test "#suggest async", %{conn: conn} do
      assert [{1, ["query"]}, {2, ["query"]}, {3, ["query"]}] ==
        request_async(conn, 3, fn(_) ->
          %Knux.Request.Suggest{collection: "knux", bucket: "default", word: "query"}
        end)
    end
  end

  describe "mode = :ingest" do
    setup [:setup_cleanup, :setup_populate, :setup_consolidate]

    setup do
      {:ok, conn} = Knux.start_link(@uri, mode: :ingest)
      {:ok, conn: conn}
    end

    test "#push", %{conn: conn} do
      assert "OK" = Knux.request(conn, %Knux.Request.Push{collection: "knux", bucket: "default", object: "object:1", text: "some text"})
    end

    test "#push buffer overflow", %{conn: conn} do
      iodata = make_long_text(1000)
      object = IO.iodata_to_binary(["started "] ++ iodata ++ [" finished"])

      assert {:error, :tcp_closed} = Knux.request(conn, %Knux.Request.Push{collection: "knux", bucket: "default", object: "object:2", text: object})
    end

    test "#push async", %{conn: conn} do
      assert [{1, "OK"}, {2, "OK"}, {3, "OK"}] ==
        request_async(conn, 3, fn(idx) ->
          %Knux.Request.Push{collection: "knux", bucket: "default", object: "object:#{idx}", text: "some text #{idx}"}
        end)
    end

    test "#pop", %{conn: conn} do
      assert "0" = Knux.request(conn, %Knux.Request.Pop{collection: "knux", bucket: "default", object: "object:1", text: "some text"})
    end

    test "#pop async", %{conn: conn} do
      assert [{1, "0"}, {2, "0"}, {3, "0"}] ==
        request_async(conn, 3, fn(idx) ->
          %Knux.Request.Pop{collection: "knux", bucket: "default", object: "object:#{idx}", text: "some text #{idx}"}
        end)
    end

    test "#count", %{conn: conn} do
      assert "1" = Knux.request(conn, %Knux.Request.Count{collection: "knux"})
      assert "2" = Knux.request(conn, %Knux.Request.Count{collection: "knux", bucket: "default"})
      assert "0" = Knux.request(conn, %Knux.Request.Count{collection: "knux", bucket: "default", object: "object:1"})
    end

    test "#count async", %{conn: conn} do
      assert [{1, "0"}, {2, "0"}, {3, "0"}] ==
        request_async(conn, 3, fn(idx) ->
          %Knux.Request.Count{collection: "knux", bucket: "default", object: "object:#{idx}"}
        end)
    end

    test "#flushc", %{conn: conn} do
      assert "1" = Knux.request(conn, %Knux.Request.Flushc{collection: "knux"})
    end

    test "#flushc async", %{conn: conn} do
      assert [{1, a}, {2, b}, {3, c}] =
        request_async(conn, 3, fn(_idx) ->
          %Knux.Request.Flushc{collection: "knux"}
        end)

      assert "1" in [a, b, c]
    end

    test "#flushb", %{conn: conn} do
      assert "1" = Knux.request(conn, %Knux.Request.Flushb{collection: "knux", bucket: "default"})
    end

    test "#flushb async", %{conn: conn} do
      assert [{1, "1"}, {2, "1"}, {3, "1"}] ==
        request_async(conn, 3, fn(_idx) ->
          %Knux.Request.Flushb{collection: "knux", bucket: "default"}
        end)
    end

    test "#flusho", %{conn: conn} do
      assert "0" = Knux.request(conn, %Knux.Request.Flusho{collection: "knux", bucket: "default", object: "object"})
    end

    test "#flusho async", %{conn: conn} do
      assert [{1, "0"}, {2, "0"}, {3, "0"}] ==
        request_async(conn, 3, fn(_idx) ->
          %Knux.Request.Flusho{collection: "knux", bucket: "default", object: "object"}
        end)
    end
  end

  describe "mode = :control" do
    setup do
      {:ok, conn} = Knux.start_link(@uri, mode: :control)
      {:ok, conn: conn}
    end

    test "#trigger", %{conn: conn} do
      assert "actions(consolidate, backup, restore)" = Knux.request(conn, %Knux.Request.Trigger{})
      assert "OK" = Knux.request(conn, %Knux.Request.Trigger{action: "consolidate"})
    end

    test "#trigger async", %{conn: conn} do
      assert [{1, "OK"}, {2, "OK"}, {3, "OK"}] ==
        request_async(conn, 3, fn(_idx) ->
          %Knux.Request.Trigger{action: "consolidate"}
        end)
    end

    @tag :skip
    test "#info", %{conn: conn} do
      # TODO parse response
      assert "uptime(33642) clients_connected(2) commands_total(347) command_latency_best(1) " <>
             "command_latency_worst(53) kv_open_count(1) fst_open_count(0) fst_consolidate_count(0)"
        = Knux.request(conn, %Knux.Request.Info{})
    end

    @tag :skip
    test "#info async", %{conn: conn} do
      # TODO parse response
      assert [{1, "OK"}, {2, "OK"}, {3, "OK"}] ==
        request_async(conn, 3, fn(_idx) ->
          %Knux.Request.Info{}
        end)
    end
  end

  describe "mode = * (commands that works in all modes)" do
    setup do
      conns = Enum.map([:search, :ingest, :control], fn(mode) ->
        {:ok, conn} = Knux.start_link(@uri, mode: mode)
        conn
      end)

      {:ok, conns: conns}
    end

    test "#ping", %{conns: conns} do
      Enum.map(conns, fn(conn) ->
        assert "PONG" = Knux.request(conn, %Knux.Request.Ping{})
      end)
    end

    test "#ping async", %{conns: conns} do
      assert Enum.map(conns, fn(_) -> [{1, "PONG"}, {2, "PONG"}, {3, "PONG"}] end) ==
        Enum.map(conns, fn(conn) ->
          request_async(conn, 3, fn(_) ->
            %Knux.Request.Ping{}
          end)
        end)
    end

    test "#help", %{conns: conns} do
      Enum.map(conns, fn(conn) ->
        assert "manuals(commands)" = Knux.request(conn, %Knux.Request.Help{})
        assert _ = Knux.request(conn, %Knux.Request.Help{manual: "commands"})
      end)
    end

    test "#help async", %{conns: conns} do
      assert Enum.map(conns, fn(_) -> [{1, "manuals(commands)"}, {2, "manuals(commands)"}, {3, "manuals(commands)"}] end) ==
        Enum.map(conns, fn(conn) ->
          request_async(conn, 3, fn(_) ->
            %Knux.Request.Help{}
          end)
        end)
    end

    test "#quit", %{conns: conns} do
      Enum.map(conns, fn(conn) ->
        assert "quit" = Knux.request(conn, %Knux.Request.Quit{})
      end)
    end
  end

  defp setup_cleanup(_context) do
    assert {:ok, conn} = Knux.start_link(@uri, mode: :ingest)
    assert _ = Knux.request(conn, %Knux.Request.Flushc{collection: "knux"})

    {:ok, conn_ingest: conn}
  end

  defp setup_populate(%{conn_ingest: conn}) do
    assert "OK" = Knux.request(conn,
      %Knux.Request.Push{collection: "knux", bucket: "default", object: "some:1", text: "some text with word - query"})

    :ok
  end

  defp setup_consolidate(_context) do
    assert {:ok, conn} = Knux.start_link(@uri, mode: :control)
    assert "OK" = Knux.request(conn, %Knux.Request.Trigger{action: "consolidate"})

    {:ok, conn_control: conn}
  end

  defp request_async(conn, count, request_fn) do
    refs =
      Enum.map(1..count, fn(idx) ->
        Task.async(fn ->
          {idx, Knux.request(conn, request_fn.(idx))}
        end)
      end)

    Enum.map(refs, fn(ref) ->
      assert {_idx, _reply} = Task.await(ref)
    end)
  end

  defp make_long_text(count) do
    for i <- 0..count, into: [] do
      "very long text that cause buffer overflow ##{i} "
    end
  end
end
