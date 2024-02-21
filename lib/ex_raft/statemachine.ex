defmodule ExRaft.Statemachine do
  @moduledoc """
  The statemachine is the component of the Raft system that is responsible for
  applying commands to the state of the system. It is the only component that
  can modify the state of the system. The statemachine is a callback module that
  is called by the Raft server when a new command is committed to the log.
  """
  alias ExRaft.Models

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(statemachine: t()) :: on_start()
  @callback handle_commands(impl :: t(), commands :: [Models.CommandEntry.t()]) :: :ok | {:error, term()}
  @callback read(impl :: t(), req :: term()) :: {:ok, term()} | {:error, term()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = statemachine), do: apply(module, :start_link, [[statemachine: statemachine]])

  @spec handle_commands(t(), [Models.CommandEntry.t()]) :: :ok | {:error, term()}
  def handle_commands(m, commands), do: delegate(m, :handle_commands, [commands])

  @spec read(t(), term()) :: {:ok, term()} | {:error, term()}
  def read(m, req), do: delegate(m, :read, [req])
end
