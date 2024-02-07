defmodule ExRaft.Rpc do
  @moduledoc """
  rpc behaviors
  """

  alias ExRaft.Models

  @type t :: any()
  @typedoc """
  RPC module which implements the `ExRaft.Rpc` behavior
  """

  @type request_t :: Models.RequestVote.Req.t() | Models.AppendEntries.Req.t()
  @type response_t :: Models.RequestVote.Reply.t() | Models.AppendEntries.Reply.t()

  @callback connect(peer :: Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  @callback call(peer :: Models.Replica.t(), req :: request_t(), timeout :: non_neg_integer()) ::
              {:ok, response_t()} | {:error, ExRaft.Exception.t()}

  defmacro __using__(_) do
    quote do
      @behaviour ExRaft.Rpc

      @impl true
      def connect(peer), do: raise("not implemented")

      @impl true
      def call(peer, req, timeout), do: raise("not implemented")

      defoverridable(connect: 1, call: 3)
    end
  end

  defp delegate(impl, func, args), do: apply(impl, func, args)

  @spec connect(t(), Models.Replica.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def connect(impl, peer), do: delegate(impl, :connect, [peer])

  @spec call(t(), Models.Replica.t(), request_t(), non_neg_integer()) ::
          {:ok, response_t()} | {:error, ExRaft.Exception.t()}
  def call(impl, peer, req, timeout), do: delegate(impl, :call, [peer, req, timeout])

  @spec just_call(t(), Models.Replica.t(), request_t(), non_neg_integer()) ::
          response_t() | ExRaft.Exception.t()
  def just_call(impl, peer, req, timeout \\ 200) do
    impl
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

  use ExRaft.Rpc

  alias ExRaft.Models

  def connect(%Models.Replica{erl_node: erl_node} = node) do
    erl_node
    |> Node.connect()
    |> if do
      :ok
    else
      {:error, ExRaft.Exception.new("connect_failed", node)}
    end
  end

  def call(%Models.Replica{erl_node: node} = replica, req, timeout) do
    node
    |> Node.ping()
    |> case do
      :pong ->
        replica
        |> Models.Replica.server()
        |> GenServer.call({:rpc_call, req}, timeout)

      :pang ->
        {:error, ExRaft.Exception.new("node not connected", node)}
    end
  end
end
