defmodule ExRaft.LogStore do
  @moduledoc """
  Log Store
  """
  alias ExRaft.Exception
  alias ExRaft.Pb
  alias ExRaft.Typespecs

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(log_store: t()) :: on_start()
  @callback stop(t()) :: :ok

  @callback append_log_entries(m :: t(), entries :: list(Typespecs.entry_t())) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback get_last_log_entry(m :: t()) ::
              {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}

  @callback get_first_log_entry(m :: t()) ::
              {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}

  @callback get_log_entry(m :: t(), index :: Typespecs.index_t()) ::
              {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}

  @callback get_log_size(m :: t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback truncate_before(m :: t(), before :: non_neg_integer()) :: :ok | {:error, Exception.t()}

  @callback get_limit(m :: t(), since :: Typespecs.index_t(), limit :: non_neg_integer()) ::
              {:ok, list(Typespecs.entry_t())} | {:error, Exception.t()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = m), do: apply(module, :start_link, [[log_store: m]])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])

  @spec append_log_entries(t(), list(Typespecs.entry_t())) ::
          {:ok, non_neg_integer()} | {:error, Exception.t()}
  def append_log_entries(m, entries), do: delegate(m, :append_log_entries, [entries])

  @spec get_last_log_entry(t()) :: {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}
  def get_last_log_entry(m), do: delegate(m, :get_last_log_entry, [])

  @spec get_first_log_entry(t()) ::
          {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}
  def get_first_log_entry(m), do: delegate(m, :get_first_log_entry, [])

  @spec get_log_size(t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def get_log_size(m), do: delegate(m, :get_log_size, [])

  @spec get_log_entry(t(), Typespecs.index_t()) :: {:ok, Typespecs.entry_t() | nil} | {:error, Exception.t()}
  def get_log_entry(m, index), do: delegate(m, :get_log_entry, [index])

  @spec truncate_before(t(), non_neg_integer()) :: :ok | {:error, Exception.t()}
  def truncate_before(m, before), do: delegate(m, :truncate_before, [before])

  @spec get_limit(t(), Typespecs.index_t(), non_neg_integer()) ::
          {:ok, list(Typespecs.entry_t())} | {:error, Exception.t()}
  def get_limit(m, since, limit), do: delegate(m, :get_limit, [since, limit])

  @spec get_log_term(t(), Typespecs.index_t()) :: {:ok, Typespecs.index_t()}
  def get_log_term(_, 0), do: {:ok, 0}

  def get_log_term(m, index) do
    m
    |> get_log_entry(index)
    |> case do
      {:ok, %Pb.Entry{term: term}} -> {:ok, term}
      {:ok, nil} -> {:ok, 0}
      {:error, _} -> {:ok, 0}
    end
  end

  @spec get_first_index(t()) :: {:ok, Typespecs.index_t()} | {:error, Exception.t()}
  def get_first_index(m) do
    m
    |> get_first_log_entry()
    |> case do
      {:ok, %Pb.Entry{index: index}} -> {:ok, index}
      {:ok, nil} -> {:ok, 0}
      {:error, _} = err -> err
    end
  end

  @spec get_last_index(t()) :: {:ok, Typespecs.index_t()} | {:error, Exception.t()}
  def get_last_index(m) do
    m
    |> get_last_log_entry()
    |> case do
      {:ok, %Pb.Entry{index: index}} -> {:ok, index}
      {:ok, nil} -> {:ok, 0}
      {:error, _} = err -> err
    end
  end
end
