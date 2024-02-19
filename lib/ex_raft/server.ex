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

  @server_opts_schema [
    id: [
      type: :non_neg_integer,
      required: true,
      doc: "Replica ID"
    ],
    peers: [
      type: {:list, :any},
      default: [],
      doc: "Replica peers, list of `{id :: non_neg_integer(), host :: String.t(), port :: non_neg_integer()}`"
    ],
    term: [
      type: :non_neg_integer,
      default: 0,
      doc: "Replica term"
    ],
    rpc_impl: [
      type: :any,
      default: ExRaft.Rpc.Default.new(),
      doc: "RPC implementation of `ExRaft.Rpc`"
    ],
    election_timeout: [
      type: :non_neg_integer,
      default: 150,
      doc: "Election timeout in milliseconds, default 150ms~300ms"
    ],
    election_check_delta: [
      type: :non_neg_integer,
      default: 15,
      doc: "Election check delta in milliseconds, default 15ms"
    ],
    heartbeat_delta: [
      type: :non_neg_integer,
      default: 50,
      doc: "Heartbeat delta in milliseconds, default 50ms"
    ]
  ]

  @type server_opts_t :: [unquote(NimbleOptions.option_typespec(@server_opts_schema))]

  # Client

  @spec start_link(server_opts_t()) :: GenServer.on_start()
  def start_link(opts) do
    opts = NimbleOptions.validate!(opts, @server_opts_schema)
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec rpc_call(GenServer.server(), ExRaft.Rpc.request_t()) ::
          {:ok, ExRaft.Rpc.response_t()} | {:error, ExRaft.Exception.t()}
  def rpc_call(pid, req) do
    GenServer.call(pid, {:rpc_call, req})
  end

  @spec show_cluster_info(GenServer.server()) :: {:ok, ExRaft.Replica.State.t()} | {:error, any()}
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
