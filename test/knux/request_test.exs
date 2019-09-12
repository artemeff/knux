defmodule Knux.RequestTest do
  use ExUnit.Case

  doctest Knux.Request

  alias Knux.Request

  describe "#encode" do
    test "encodes simple request" do
      assert %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "text"}]}
          == Request.encode(%Request.Push{collection: "c", bucket: "b", object: "o", text: "text"})
    end
  end

  describe "#chunk" do
    test "splits request that overflow provided io_size" do
      text = """
      Lorem ipsum dolor sit amet, consectetur adipiscing elit,
      sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
      Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris
      nisi ut aliquip ex ea commodo consequat.
      """

      request = %Request.Push{collection: "c", bucket: "b", object: "o", text: text}

      assert [
        %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod"}]},
        %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "tempor incididunt ut labore et dolore magna aliqua Ut enim ad minim"}]},
        %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea"}]},
        %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "commodo consequat"}]},
      ] == Request.chunk(request, 100) |> Enum.map(&Request.encode/1)
    end

    test "splits request that can overflow provided io_size, but dont" do
      text = """
      Lorem ipsum dolor sit amet, consectetur adipiscing elit.
      """

      request = %Request.Push{collection: "c", bucket: "b", object: "o", text: text}

      assert [
        %Request{mode: :ingest, io_data: ["PUSH", "c", "b", "o", {:quoted, "Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n"}]},
      ] == Request.chunk(request, 100) |> Enum.map(&Request.encode/1)
    end

    test "returns as is if request doesn't have quoted arguments" do
      request = %Request.Flushc{collection: "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit"}

      assert [
        %Request{mode: :ingest, io_data: ["FLUSHC", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit"]},
      ] == Request.chunk(request, 100) |> Enum.map(&Request.encode/1)
    end
  end
end
