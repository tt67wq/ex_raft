defmodule ExRaft.Server do
  @moduledoc """
  Raft Server
  """

  use GenServer

  defmodule State do
    @moduledoc """
    Raft Server State
    """

    @type t :: %__MODULE__{
            replica_pid: pid()
          }

    defstruct replica_pid: nil
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec rpc_call(GenServer.server(), ExRaft.Rpc.request_t()) ::
          {:ok, ExRaft.Rpc.response_t()} | {:error, ExRaft.Exception.t()}
  def rpc_call(pid, req) do
    GenServer.call(pid, {:rpc_call, req})
  end

  @spec show_cluster_info(GenServer.server()) :: {:ok, ExRaft.Cluster.info_t()}
  def show_cluster_info(pid) do
    GenServer.call(pid, :show_cluster_info)
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
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call({:rpc_call, req}, _from, %{replica_pid: replica_pid} = state) do
    {:reply, :gen_statem.call(replica_pid, {:rpc_call, req}), state}
  end

  def handle_call(:show_cluster_info, _from, %{replica_pid: replica_pid} = state) do
    {:reply, :gen_statem.call(replica_pid, :show), state}
  end
end
