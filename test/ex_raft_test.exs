defmodule ExRaftTest do
  use ExUnit.Case
  doctest ExRaft

  test "greets the world" do
    assert ExRaft.hello() == :world
  end
end
