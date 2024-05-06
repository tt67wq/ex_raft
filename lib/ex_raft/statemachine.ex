defmodule ExRaft.Statemachine do
  @moduledoc """
  The statemachine is the component of the Raft system that is responsible for
  applying entries to the state of the system. It is the only component that
  can modify the state of the system. The statemachine is a callback module that
  is called by the Raft server when a new command is committed to the log.
  """
  alias ExRaft.Typespecs

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}
  @type err_t :: {:error, term()} | :error

  @type safe_point :: {Typespecs.index_t(), Typespecs.term_t(), term()}

  @callback start_link(statemachine: t()) :: on_start()
  @callback stop(t()) :: :ok
  @callback update(impl :: t(), entries :: [Typespecs.entry_t()]) :: :ok | err_t()
  @callback read(impl :: t(), req :: term()) :: {:ok, term()} | err_t()
  @callback prepare_snapshot(impl :: t()) :: {:ok, safe_point()} | err_t()
  @callback save_snapshot(impl :: t(), safe_point :: safe_point(), io_device :: IO.device()) :: :ok | err_t()
  @callback load_snapshot(impl :: t(), io_device :: IO.device()) :: :ok | err_t()

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = statemachine), do: apply(module, :start_link, [[statemachine: statemachine]])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])

  @spec update(t(), [Typespecs.entry_t()]) :: :ok | err_t()
  def update(m, entries), do: delegate(m, :update, [entries])

  @spec read(t(), term()) :: {:ok, term()} | err_t()
  def read(m, req), do: delegate(m, :read, [req])

  @spec prepare_snapshot(t()) :: {:ok, safe_point()} | err_t()
  def prepare_snapshot(m), do: delegate(m, :prepare_snapshot, [])

  @spec save_snapshot(t(), safe_point(), IO.device()) :: :ok | err_t()
  def save_snapshot(m, safe_point, io_device), do: delegate(m, :save_snapshot, [safe_point, io_device])

  @spec load_snapshot(t(), IO.device()) :: :ok | err_t()
  def load_snapshot(m, io_device), do: delegate(m, :load_snapshot, [io_device])
end
