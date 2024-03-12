defmodule ExRaft.LogStore.Cub do
  @moduledoc """
  Log Store implementation using CubDB
  """

  @behaviour ExRaft.LogStore

  use Agent

  alias ExRaft.Pb

  @type t :: %__MODULE__{
          name: atom(),
          data_dir: String.t()
        }

  @min_key :min_index
  @max_key :max_index

  defstruct name: __MODULE__, data_dir: "./data"

  def new(opts \\ []) do
    opts = opts |> Keyword.put_new(:name, __MODULE__) |> Keyword.put_new(:data_dir, "./data")
    struct(__MODULE__, opts)
  end

  @impl ExRaft.LogStore
  def start_link(log_store: %__MODULE__{name: name, data_dir: data_dir}) do
    Agent.start_link(
      fn ->
        {:ok, pid} = CubDB.start_link(data_dir: data_dir)
        pid
      end,
      name: name
    )
  end

  @impl ExRaft.LogStore
  def stop(%__MODULE__{name: name}) do
    Agent.cast(name, fn db -> CubDB.stop(db) end)
    Agent.stop(name)
  end

  @impl ExRaft.LogStore
  def append_log_entries(_, []), do: {:ok, 0}

  def append_log_entries(%__MODULE__{name: name}, entries) do
    {Agent.cast(name, __MODULE__, :handle_append_log_entries, [entries]), Enum.count(entries)}
  end

  @impl ExRaft.LogStore
  def get_first_log_entry(%__MODULE__{name: name}), do: Agent.get(name, __MODULE__, :handle_get_first_log_entry, [])

  @impl ExRaft.LogStore
  def get_last_log_entry(%__MODULE__{name: name}), do: Agent.get(name, __MODULE__, :handle_get_last_log_entry, [])

  @impl ExRaft.LogStore
  def get_log_entry(%__MODULE__{name: name}, index), do: Agent.get(name, __MODULE__, :handle_get_log_entry, [index])

  @impl ExRaft.LogStore
  def truncate_before(%__MODULE__{name: name}, before),
    do: Agent.cast(name, __MODULE__, :handle_truncate_before, [before])

  @impl ExRaft.LogStore
  def get_limit(%__MODULE__{name: name}, since, limit), do: Agent.get(name, __MODULE__, :handle_get_limit, [since, limit])
  # -------------- handlers --------------

  def handle_append_log_entries(db, entries) do
    [%Pb.Entry{index: f}] = entries
    %Pb.Entry{index: l} = List.last(entries)
    ms = Map.new(entries, fn %Pb.Entry{index: index} = entry -> {index, entry} end)
    :ok = CubDB.put_multi(db, ms)

    may_update_min_index(db, f)
    update_max_index(db, l)

    db
  end

  def handle_truncate_before(db, before) do
    fi = first_index(db)

    if fi < before do
      CubDB.put(db, @min_key, before)
      CubDB.delete_multi(db, Enum.to_list(fi..before))
    end

    db
  end

  def handle_get_first_log_entry(db) do
    {:ok, CubDB.get(db, @min_key)}
  end

  def handle_get_last_log_entry(db) do
    {:ok, CubDB.get(db, @max_key)}
  end

  def handle_get_log_entry(db, index) do
    {:ok, CubDB.get(db, index)}
  end

  def handle_get_limit(db, since, limit) do
    before = min(since + limit, last_index(db))
    {:ok, get_range(db, since, before)}
  end

  # --------------- private ---------------

  defp may_update_min_index(db, m) do
    db
    |> CubDB.has_key?(@min_key)
    |> if do
      CubDB.put(db, @min_key, m)
    end
  end

  defp update_max_index(db, m) do
    CubDB.put(db, @max_key, m)
  end

  defp first_index(db) do
    db
    |> CubDB.get(@min_key)
    |> case do
      nil -> 0
      %Pb.Entry{index: index} -> index
    end
  end

  defp last_index(db) do
    db
    |> CubDB.get(@max_key)
    |> case do
      nil -> 0
      %Pb.Entry{index: index} -> index
    end
  end

  defp get_range(_db, since, before) when since == before, do: []

  defp get_range(db, since, before) do
    db
    |> CubDB.select(min_key: since + 1, max_key: before)
    |> Enum.to_list()
  end
end
