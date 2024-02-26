defmodule ExRaft.LogStore.Inmem do
  @moduledoc """
  In-Memory Log Store
  """
  @behaviour ExRaft.LogStore

  use Agent

  alias ExRaft.Exception

  defstruct name: __MODULE__

  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    struct(__MODULE__, opts)
  end

  @impl ExRaft.LogStore
  def start_link(log_store: %__MODULE__{name: name}) do
    table = :"#{name}_table"
    Agent.start_link(fn -> :ets.new(table, [:named_table, :set]) end, name: name)
  end

  @impl ExRaft.LogStore
  def append_log_entries(%__MODULE__{name: name}, prev_index, entries) do
    {Agent.cast(name, __MODULE__, :handle_append_log_entries, [prev_index, entries]), Enum.count(entries)}
  end

  @impl ExRaft.LogStore
  def get_last_log_entry(%__MODULE__{name: name}) do
    Agent.get(name, __MODULE__, :handle_get_last_log_entry, [])
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
  def get_range(%__MODULE__{name: name}, since, before) do
    Agent.get(name, __MODULE__, :handle_get_range, [since, before])
  end

  def handle_append_log_entries(table, _, []), do: table

  def handle_append_log_entries(table, prev_index, entries) do
    entries
    |> Enum.with_index(prev_index + 1)
    |> Enum.each(fn {entry, index} -> :ets.insert(table, {index, entry}) end)

    table
  end

  def handle_get_last_log_entry(table) do
    table
    |> :ets.last()
    |> case do
      :"$end_of_table" ->
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

  def handle_get_log_entry(table, index) do
    table
    |> :ets.lookup(index)
    |> case do
      [{_, entry}] -> {:ok, entry}
      _ -> {:ok, nil}
    end
  end

  def handle_truncate_before(table, before) do
    :ets.select_delete(table, [{{:"$1", :_}, [{:>, :"$1", before}], [false]}, {:_, [], [true]}])
    table
  end

  def handle_get_range(_table, since, before) when since == before do
    {:ok, []}
  end

  def handle_get_range(table, since, before) do
    selector = [
      {{:"$1", :_}, [{:andalso, {:>, :"$1", since}, {:<=, :"$1", before}}], [:"$_"]},
      {:_, [], [nil]}
    ]

    entries =
      table
      |> :ets.select(selector)
      |> Enum.reject(fn
        nil -> true
        _ -> false
      end)
      |> Enum.map(fn {_, entry} -> entry end)

    {:ok, entries}
  end
end
