defmodule ExRaft.Server do
  @moduledoc """
  Raft Server
  """

  use GenServer

  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

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
    remote_impl: [
      type: :any,
      doc: "Implementation of `ExRaft.Remote`"
    ],
    log_store_impl: [
      type: :any,
      doc: "Log Store implementation of `ExRaft.LogStore`"
    ],
    statemachine_impl: [
      type: :any,
      required: true,
      doc: "Statemachine implementation of `ExRaft.Statemachine`"
    ],
    tick_delta: [
      type: :non_neg_integer,
      default: 100,
      doc: "Local tick delta in milliseconds, default 100ms"
    ],
    election_timeout: [
      type: :non_neg_integer,
      default: 10,
      doc:
        "Election timeout threshold is a multiple of local_tick. For example, if local_tick is 10ms and I want to initiate a new election every 100ms, this value should be set to 10."
    ],
    heartbeat_timeout: [
      type: :non_neg_integer,
      default: 2,
      doc:
        "Heartbeat timeout threshold is a multiple of local_tick. For example, if local_tick is 10ms and I want to initiate a heartbeat every 100ms, this value should be set to 10."
    ]
  ]

  @type server_opts_t :: [unquote(NimbleOptions.option_typespec(@server_opts_schema))]

  # Client

  @spec start_link(server_opts_t()) :: GenServer.on_start()
  def start_link(opts) do
    opts =
      opts
      |> Keyword.put_new_lazy(:remote_impl, fn -> ExRaft.Remote.Erlang.new() end)
      |> Keyword.put_new_lazy(:log_store_impl, fn -> ExRaft.LogStore.Cub.new(data_dir: "./raft_data") end)
      |> Keyword.put_new_lazy(:statemachine_impl, fn -> ExRaft.Mock.Statemachine.new() end)
      |> NimbleOptions.validate!(@server_opts_schema)

    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec show_cluster_info() :: {:ok, ReplicaState.t()} | {:error, any()}
  def show_cluster_info do
    GenServer.call(__MODULE__, :show_cluster_info)
  end

  @spec propose(list(binary())) :: :ok | {:error, any()}
  def propose(cmds) do
    GenServer.cast(__MODULE__, {:propose, cmds})
  end

  # Server (callbacks)

  @impl true
  def init(opts) do
    {:ok, replica_pid} = ExRaft.Replica.start_link(opts)
    {:ok, %{replica_pid: replica_pid}}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call(:show_cluster_info, _from, %{replica_pid: replica_pid} = state) do
    {:reply, :gen_statem.call(replica_pid, :show), state}
  end

  def handle_cast({:propose, cmds}, %{replica_pid: replica_pid} = state) do
    entries = Enum.map(cmds, fn cmd -> %Pb.Entry{cmd: cmd} end)
    :gen_statem.cast(replica_pid, {:propose, entries})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:pipeout, body}, %{replica_pid: replica_pid} = state) do
    body
    |> ExRaft.Serialize.batch_decode(ExRaft.Pb.Message)
    |> Enum.each(fn msg ->
      :gen_statem.cast(replica_pid, {:pipein, msg})
    end)

    {:noreply, state}
  end
end
