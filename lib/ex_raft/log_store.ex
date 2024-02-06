defmodule ExRaft.LogStore do
  @moduledoc """
  Log Store
  """
  alias ExRaft.Exception
  alias ExRaft.Models

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(log_store: t()) :: on_start()

  @callback get_log_entries(m :: t(), since :: non_neg_integer(), before :: non_neg_integer()) ::
              {:ok, [Models.LogEntry.t()]} | {:error, Exception.t()}

  @callback append_log_entries(m :: t(), entries :: list(Models.LogEntry.t())) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback get_last_log_entry(m :: t()) ::
              {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}

  @callback get_log_entry(m :: t(), index :: non_neg_integer()) ::
              {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}

  @callback truncate_before(m :: t(), before :: non_neg_integer()) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = m), do: apply(module, :start_link, [[log_store: m]])

  @spec get_log_entries(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [Models.LogEntry.t()]} | {:error, Exception.t()}
  def get_log_entries(m, since, before), do: delegate(m, :get_log_entries, [since, before])

  @spec append_log_entries(t(), list(Models.LogEntry.t())) ::
          {:ok, non_neg_integer()} | {:error, Exception.t()}
  def append_log_entries(m, entries), do: delegate(m, :append_log_entries, [entries])

  @spec get_last_log_entry(t()) :: {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}
  def get_last_log_entry(m), do: delegate(m, :get_last_log_entry, [])

  @spec get_log_entry(t(), non_neg_integer()) :: {:ok, Models.LogEntry.t() | nil} | {:error, Exception.t()}
  def get_log_entry(m, index), do: delegate(m, :get_log_entry, [index])
end
