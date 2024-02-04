defmodule ExRaft.Rpc do
  @moduledoc """
  rpc behaviors
  """

  alias ExRaft.Models

  @type t :: struct()
  @type request_t :: Models.RequestVote.Req.t() | Models.AppendEntries.Req.t()
  @type response_t :: Models.RequestVote.Reply.t() | Models.AppendEntries.Reply.t()

  @callback call(m :: t(), peer :: Models.Replica.t(), req :: request_t(), timeout :: non_neg_integer()) ::
              {:ok, response_t()} | {:error, ExRaft.Exception.t()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

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

defmodule ExRaft.Rpc.Default do
  @moduledoc """
  Default rpc implementation
  """

  @behaviour ExRaft.Rpc

  alias ExRaft.Models

  defstruct []

  def new, do: %__MODULE__{}

  @impl true
  def call(%__MODULE__{}, %Models.Replica{name: name}, req, timeout) do
    GenServer.call({ExRaft.Server, name}, {:call, req}, timeout)
  end
end
