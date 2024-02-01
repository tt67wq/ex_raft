defmodule ExRaft.Rpc do
  @moduledoc """
  rpc behaviors
  """

  alias ExRaft.Models

  @type t :: struct()
  @type request_t :: Models.RequestVote.t() | Models.AppendEntries.t()
  @type response_t :: Models.RequestVoteResponse.t() | Models.AppendEntriesResponse.t()

  @callback call(m :: t(), peer :: Models.Replica.t(), req :: request_t(), timeout :: non_neg_integer()) ::
              {:ok, response_t()} | {:error, ExRaft.Exception.t()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec call(t(), Models.Replica.t(), request_t(), non_neg_integer()) ::
          {:ok, response_t()} | {:error, ExRaft.Exception.t()}
  def call(m, peer, req, timeout \\ 200), do: delegate(m, :call, [peer, req, timeout])
end
