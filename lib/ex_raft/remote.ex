defmodule ExRaft.Remote do
  @moduledoc """
  Remote behaviors
  """

  alias ExRaft.Models
  alias ExRaft.Typespecs

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(remote: t()) :: on_start()
  @callback stop(t()) :: :ok
  @callback connect(m :: t(), peer :: Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  @callback disconnect(m :: t(), peer :: Models.Replica.t()) :: :ok
  @callback pipeout(m :: t(), ms :: [Typespecs.message_t()]) ::
              :ok | {:error, ExRaft.Exception.t()}

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = remote), do: apply(module, :start_link, [[remote: remote]])

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])

  @spec connect(t(), Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def connect(m, peer), do: delegate(m, :connect, [peer])

  @spec disconnect(t(), Models.Replica.t()) :: :ok
  def disconnect(m, peer), do: delegate(m, :disconnect, [peer])

  @spec pipeout(t(), [Typespecs.message_t()]) :: :ok | {:error, ExRaft.Exception.t()}
  def pipeout(_m, []), do: :ok
  def pipeout(m, ms), do: delegate(m, :pipeout, [ms])
end
