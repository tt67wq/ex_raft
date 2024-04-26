defmodule ExRaft.LogStore.Inmem do
  @moduledoc """
  In-Memory Log Store.
  """
  @behaviour ExRaft.LogStore

  use Agent

  alias ExRaft.Exception
  alias ExRaft.Pb

  defstruct name: __MODULE__

  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    struct(__MODULE__, opts)
  end

  @impl ExRaft.LogStore
  def start_link(log_store: %__MODULE__{name: name}) do
    table = :"#{name}_table"
    Agent.start_link(fn -> :ets.new(table, [:named_table, :ordered_set]) end, name: name)
  end

  @impl ExRaft.LogStore
  def stop(%__MODULE__{name: name}) do
    Agent.stop(name)
  end

  @impl ExRaft.LogStore
  def append_log_entries(%__MODULE__{name: name}, entries) do
    {Agent.cast(name, __MODULE__, :handle_append_log_entries, [entries]), Enum.count(entries)}
  end

  @impl ExRaft.LogStore
  def get_last_log_entry(%__MODULE__{name: name}) do
    Agent.get(name, __MODULE__, :handle_get_last_log_entry, [])
  end

  @impl ExRaft.LogStore
  def get_first_log_entry(%__MODULE__{name: name}) do
    Agent.get(name, __MODULE__, :handle_get_first_log_entry, [])
  end

  @impl ExRaft.LogStore
  def get_log_entry(%__MODULE__{name: name}, index) do
    Agent.get(name, __MODULE__, :handle_get_log_entry, [index])
  end

  @impl ExRaft.LogStore
  def truncate_before(%__MODULE__{name: name}, before) do
    Agent.cast(name, __MODULE__, :handle_truncate_before, [before])
  end

  @impl ExRaft.LogStore
  def get_limit(%__MODULE__{name: name}, since, limit) do
    Agent.get(name, __MODULE__, :handle_get_limit, [since, limit])
  end

  @impl ExRaft.LogStore
  def get_log_size(%__MODULE__{name: name}) do
    Agent.get(name, __MODULE__, :handle_get_log_size, [])
  end

  # ---------------------- agent handlers -----------------

  def handle_append_log_entries(table, []), do: table

  def handle_append_log_entries(table, entries) do
    Enum.each(entries, fn %Pb.Entry{index: index} = entry -> :ets.insert(table, {index, entry}) end)
    table
  end

  def handle_get_last_log_entry(table) do
    table
    |> last_index()
    |> case do
      -1 ->
        {:ok, nil}

      last_index ->
        table
        |> :ets.lookup(last_index)
        |> case do
          [{_, entry}] -> {:ok, entry}
          _ -> {:error, Exception.new("not_found", last_index)}
        end
    end
  end

  def handle_get_first_log_entry(table) do
    table
    |> first_index()
    |> case do
      -1 ->
        {:ok, nil}

      first_index ->
        table
        |> :ets.lookup(first_index)
        |> case do
          [{_, entry}] -> {:ok, entry}
          _ -> {:error, Exception.new("not_found", first_index)}
        end
    end
  end

  defp last_index(table) do
    table
    |> :ets.last()
    |> case do
      :"$end_of_table" -> -1
      last_index -> last_index
    end
  end

  defp first_index(table) do
    table
    |> :ets.first()
    |> case do
      :"$end_of_table" -> -1
      first_index -> first_index
    end
  end

  def handle_get_log_entry(table, index) do
    {:ok, get_one(table, index)}
  end

  def handle_truncate_before(table, before) do
    :ets.select_delete(table, [{{:"$1", :_}, [{:>, :"$1", before}], [false]}, {:_, [], [true]}])
    table
  end

  defp get_range(_table, since, before) when since == before do
    {:ok, []}
  end

  defp get_range(table, since, before) do
    ret =
      (since + 1)..before
      |> Enum.map(fn index -> get_one(table, index) end)
      |> Enum.reject(&is_nil(&1))

    {:ok, ret}
  end

  def handle_get_limit(_table, _since, 0), do: {:ok, []}

  def handle_get_limit(table, since, limit) do
    before = min(since + limit, last_index(table))
    get_range(table, since, before)
  end

  defp get_one(table, index) do
    table
    |> :ets.lookup(index)
    |> case do
      [{_, entry}] -> entry
      _ -> nil
    end
  end

  def handle_get_log_size(table) do
    {:ok, last_index(table) - first_index(table)}
  end
end
