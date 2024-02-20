defmodule ExRaft.Rpc do
  @moduledoc """
  rpc behaviors
  """

  alias ExRaft.Models

  @type t :: struct()
  @type request_t :: Models.RequestVote.Req.t() | Models.AppendEntries.Req.t() | Models.PreRequestVote.Req.t()
  @type response_t :: Models.RequestVote.Reply.t() | Models.AppendEntries.Reply.t() | Models.PreRequestVote.Reply.t()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(rpc: t()) :: on_start()
  @callback connect(m :: t(), peer :: Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  @callback call(m :: t(), peer :: Models.Replica.t(), req :: request_t(), timeout :: non_neg_integer()) ::
              {:ok, response_t()} | {:error, ExRaft.Exception.t()}

  @spec start_link(t()) :: on_start()
  def start_link(rpc), do: apply(rpc, :start_link, [[rpc: rpc]])

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec connect(t(), Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def connect(m, peer), do: delegate(m, :connect, [peer])

  @spec connect!(t(), Models.Replica.t()) :: :ok
  def connect!(m, peer) do
    case connect(m, peer) do
      :ok -> :ok
      {:error, e} -> raise e
    end
  end

  @spec call(t(), Models.Replica.t(), request_t(), non_neg_integer()) ::
          {:ok, response_t()} | {:error, ExRaft.Exception.t()}
  def call(m, peer, req, timeout \\ 200), do: delegate(m, :call, [peer, req, timeout])

  @spec just_call(t(), Models.Replica.t(), request_t(), non_neg_integer()) ::
          response_t() | ExRaft.Exception.t()
  def just_call(m, peer, req, timeout \\ 200) do
    m
    |> call(peer, req, timeout)
    |> case do
      {:ok, res} -> res
      {:error, e} -> e
    end
  end
end
