defmodule ExRaft.Server do
  @moduledoc """
  Raft Server
  """

  # Client

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec rpc_call(GenServer.server(), ExRaft.Rpc.request_t()) ::
          {:ok, ExRaft.Rpc.response_t()} | {:error, ExRaft.Exception.t()}
  def rpc_call(pid, req) do
    GenServer.call(pid, {:rpc_call, req})
  end

  # Server (callbacks)

  @impl true
  def init(opts) do
    {:ok, replica_pid} = ExRaft.Replica.start_link(opts)

    {:ok,
     %{
       replica_pid: replica_pid
     }}
  end

  @impl true
  def handle_call({:rpc_call, req}, _from, %{replica_pid: replica_pid} = state) do
    {:reply, :gen_statem.call(replica_pid, {:rpc_call, req}), state}
  end
end
