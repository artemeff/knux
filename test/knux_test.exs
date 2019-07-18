defmodule KnuxTest do
  use ExUnit.Case
  doctest Knux

  test "greets the world" do
    assert Knux.hello() == :world
  end
end
