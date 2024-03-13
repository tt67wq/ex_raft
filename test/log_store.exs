defmodule LogStoreTest do
  @moduledoc false

  use ExUnit.Case

  alias ExRaft.LogStore
  alias ExRaft.LogStore.Cub

  setup do
    c = Cub.new(data_dir: "./tmp/data")
    start_supervised!({Cub, log_store: c})

    on_exit(fn -> File.rm_rf("./tmp/data") end)
    {:ok, log_store: c}
  end

  test "append_log_entries", %{log_store: c} do
    assert {:ok, 1} == LogStore.append_log_entries(c, [%ExRaft.Pb.Entry{index: 1}])
  end

  test "last/first", %{log_store: c} do
    assert {:ok, nil} == LogStore.get_first_log_entry(c)
    assert {:ok, nil} == LogStore.get_last_log_entry(c)

    assert {:ok, 2} == LogStore.append_log_entries(c, [%ExRaft.Pb.Entry{index: 1}, %ExRaft.Pb.Entry{index: 2}])
    assert {:ok, %ExRaft.Pb.Entry{index: 1}} == LogStore.get_first_log_entry(c)
    assert {:ok, %ExRaft.Pb.Entry{index: 2}} == LogStore.get_last_log_entry(c)
  end

  test "get_log_entry", %{log_store: c} do
    assert {:ok, nil} == LogStore.get_log_entry(c, 1)
    assert {:ok, 2} == LogStore.append_log_entries(c, [%ExRaft.Pb.Entry{index: 1}, %ExRaft.Pb.Entry{index: 2}])
    assert {:ok, %ExRaft.Pb.Entry{index: 1}} == LogStore.get_log_entry(c, 1)
  end

  test "truncate_before", %{log_store: c} do
    assert {:ok, 2} == LogStore.append_log_entries(c, [%ExRaft.Pb.Entry{index: 1}, %ExRaft.Pb.Entry{index: 2}])
    assert :ok == LogStore.truncate_before(c, 2)
    assert {:ok, %ExRaft.Pb.Entry{index: 2}} == LogStore.get_first_log_entry(c)
  end

  test "get_limit", %{log_store: c} do
    assert {:ok, []} == LogStore.get_limit(c, 1, 1)

    assert {:ok, 3} ==
             LogStore.append_log_entries(c, [
               %ExRaft.Pb.Entry{index: 1},
               %ExRaft.Pb.Entry{index: 2},
               %ExRaft.Pb.Entry{index: 3}
             ])

    assert {:ok, [%ExRaft.Pb.Entry{index: 1}]} == LogStore.get_limit(c, 0, 1)
    assert {:ok, [%ExRaft.Pb.Entry{index: 1}, %ExRaft.Pb.Entry{index: 2}]} == LogStore.get_limit(c, 0, 2)

    assert {:ok, [%ExRaft.Pb.Entry{index: 1}, %ExRaft.Pb.Entry{index: 2}, %ExRaft.Pb.Entry{index: 3}]} ==
             LogStore.get_limit(c, 0, 5)

    assert {:ok, [%ExRaft.Pb.Entry{index: 2}, %ExRaft.Pb.Entry{index: 3}]} == LogStore.get_limit(c, 1, 2)
  end
end
