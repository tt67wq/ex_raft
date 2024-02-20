defmodule ExRaft.LogStore do
  @moduledoc """
  Log Store
  """
  alias ExRaft.Exception
  alias ExRaft.Models

  @type t :: struct()
  @type index_t :: integer()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(log_store: t()) :: on_start()

  @callback append_log_entries(m :: t(), entries :: list(Models.LogEntry.t())) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback get_last_log_entry(m :: t()) ::
              {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}

  @callback get_log_entry(m :: t(), index :: index_t()) ::
              {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}

  @callback truncate_before(m :: t(), before :: non_neg_integer()) :: :ok | {:error, Exception.t()}

  @callback get_range(m :: t(), since :: index_t(), before :: index_t()) ::
              {:ok, list(Models.LogEntry.t())} | {:error, Exception.t()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = m), do: apply(module, :start_link, [[log_store: m]])

  @spec append_log_entries(t(), list(Models.LogEntry.t())) ::
          {:ok, non_neg_integer()} | {:error, Exception.t()}
  def append_log_entries(m, entries), do: delegate(m, :append_log_entries, [entries])

  @spec get_last_log_entry(t()) :: {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}
  def get_last_log_entry(m), do: delegate(m, :get_last_log_entry, [])

  @spec get_log_entry(t(), index_t()) :: {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}
  def get_log_entry(m, index), do: delegate(m, :get_log_entry, [index])

  @spec truncate_before(t(), non_neg_integer()) :: :ok | {:error, Exception.t()}
  def truncate_before(m, before), do: delegate(m, :truncate_before, [before])

  @spec get_range(t(), index_t(), index_t()) :: {:ok, list(Models.LogEntry.t())} | {:error, Exception.t()}
  def get_range(m, since, before), do: delegate(m, :get_range, [since, before])
end
