defmodule ExRaft.Pipeline do
  @moduledoc """
  Pipeline behaviors
  """

  alias ExRaft.Models

  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(pipeline: t()) :: on_start()
  @callback stop(t()) :: :ok
  @callback connect(m :: t(), peer :: Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  @callback pipeout(m :: t(), to_id :: non_neg_integer(), ms :: [Models.PackageMaterial.t()]) ::
              :ok | {:error, ExRaft.Exception.t()}

  @callback batch_pipeout(m :: t(), ms :: [{non_neg_integer(), [Models.PackageMaterial.t()]}]) ::
              :ok | {:error, ExRaft.Exception.t()}

  @spec start_link(t()) :: on_start()
  def start_link(%module{} = pipeline), do: apply(module, :start_link, [[pipeline: pipeline]])

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])

  @spec connect(t(), Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def connect(m, peer), do: delegate(m, :connect, [peer])

  @spec pipeout(t(), non_neg_integer(), [Models.PackageMaterial.t()]) :: :ok | {:error, ExRaft.Exception.t()}
  def pipeout(m, to_id, ms), do: delegate(m, :pipeout, [to_id, ms])

  @spec batch_pipeout(t(), [{non_neg_integer(), [Models.PackageMaterial.t()]}]) :: :ok | {:error, ExRaft.Exception.t()}
  def batch_pipeout(m, ms), do: delegate(m, :batch_pipeout, [ms])
end
