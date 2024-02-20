defmodule ExRaft.LogStore.Wal do
  @moduledoc """
  Log Store Implementation using Write-Ahead-Log (ExWal)
  """

  @behaviour ExRaft.LogStore

  alias ExRaft.Models

  @default_wal_opts [
    {:segment_size, 4 * 1024 * 1024},
    {:segment_cache_size, 5},
    {:nosync, true}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          path: String.t(),
          wal_opts: Keyword.t()
        }
  defstruct name: __MODULE__, path: "raft_log", wal_opts: []

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    opts =
      opts
      |> Keyword.put_new(:name, __MODULE__)
      |> Keyword.put_new(:path, "raft_log")
      |> Keyword.put_new(:wal_opts, @default_wal_opts)

    struct(__MODULE__, opts)
  end

  @impl ExRaft.LogStore
  def start_link(log_store: m) do
    opts =
      [
        name: m.name,
        path: m.path
      ] ++ m.wal_opts

    ExWal.start_link(opts)
  end

  @impl ExRaft.LogStore
  def append_log_entries(_, []), do: {:ok, 0}

  def append_log_entries(%__MODULE__{name: name}, entries) do
    wal_entries =
      Enum.map(entries, fn %Models.LogEntry{index: index} = log ->
        ExWal.Models.Entry.new(index, Models.LogEntry.encode(log), log)
      end)

    :ok = ExWal.write(name, wal_entries)
    {:ok, Enum.count(entries)}
  end

  @impl ExRaft.LogStore
  def get_last_log_entry(%__MODULE__{name: name} = m) do
    name
    |> ExWal.last_index()
    |> case do
      -1 ->
        {:ok, nil}

      index ->
        get_log_entry(m, index)
    end
  end

  @impl ExRaft.LogStore
  def get_log_entry(_, -1), do: {:ok, nil}

  def get_log_entry(%__MODULE__{name: name}, index) do
    {:ok, get_one(name, index)}
  end

  @impl ExRaft.LogStore
  def truncate_before(%__MODULE__{name: name}, before) do
    name
    |> ExWal.truncate_before(before)
    |> case do
      :ok -> :ok
      {:error, :index_out_of_range} -> {:error, ExRaft.Exception.new("index_out_of_range", before)}
    end
  end

  @impl ExRaft.LogStore
  def get_range(%__MODULE__{name: name}, since, before) do
    {:ok, Enum.map(since..before, &get_one(name, &1))}
  end

  @spec get_one(atom(), integer()) :: Models.LogEntry.t() | nil
  defp get_one(name, index) do
    case ExWal.read(name, index) do
      {:ok, %ExWal.Models.Entry{data: data, cache: nil}} ->
        Models.LogEntry.decode(data)

      {:ok, %ExWal.Models.Entry{cache: cache}} ->
        cache

      {:error, :index_not_found} ->
        nil
    end
  end
end
