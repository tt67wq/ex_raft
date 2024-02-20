defmodule ExRaft.Rpc.Erlang do
  @moduledoc """
  ExRaft.Rpc implementation using Erlang
  """
  @behaviour ExRaft.Rpc

  alias ExRaft.Models

  defstruct name: __MODULE__

  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    struct(__MODULE__, opts)
  end

  @impl ExRaft.Rpc
  def start_link(rpc: %__MODULE__{name: name} = m) do
    Agent.start_link(fn -> m end, name: name)
  end

  @impl ExRaft.Rpc
  def connect(%__MODULE__{}, %Models.Replica{erl_node: erl_node} = node) do
    erl_node
    |> Node.connect()
    |> if do
      :ok
    else
      {:error, ExRaft.Exception.new("connect_failed", node)}
    end
  end

  @impl ExRaft.Rpc
  def call(%__MODULE__{}, %Models.Replica{erl_node: node} = replica, req, timeout) do
    node
    |> Node.ping()
    |> case do
      :pong ->
        try do
          replica
          |> Models.Replica.server()
          |> GenServer.call({:rpc_call, req}, timeout)
        catch
          :exit, _ -> {:error, ExRaft.Exception.new("rpc_timeout", replica)}
          other_exception -> raise other_exception
        end

      :pang ->
        {:error, ExRaft.Exception.new("node not connected", node)}
    end
  end
end
