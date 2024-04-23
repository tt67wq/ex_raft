defmodule ExRaft.Statemachine do
  @moduledoc """
  The statemachine is the component of the Raft system that is responsible for
  applying commands to the state of the system. It is the only component that
  can modify the state of the system. The statemachine is a callback module that
  is called by the Raft server when a new command is committed to the log.
  """
  alias ExRaft.Typespecs

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(statemachine: t()) :: on_start()
  @callback stop(t()) :: :ok
  @callback update(impl :: t(), commands :: [Typespecs.entry_t()]) :: :ok | {:error, term()}
  @callback read(impl :: t(), req :: term()) :: {:ok, term()} | {:error, term()}
  @callback save_snapshot(impl :: t(), io_device :: IO.device()) :: :ok | {:error, term()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = statemachine), do: apply(module, :start_link, [[statemachine: statemachine]])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])

  @spec update(t(), [Typespecs.entry_t()]) :: :ok | {:error, term()}
  def update(m, commands), do: delegate(m, :update, [commands])

  @spec read(t(), term()) :: {:ok, term()} | {:error, term()}
  def read(m, req), do: delegate(m, :read, [req])

  @spec save_snapshot(t(), IO.device()) :: :ok | {:error, term()}
  def save_snapshot(m, io_device), do: delegate(m, :save_snapshot, [io_device])
end
