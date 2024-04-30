defmodule ExRaft.Server do
  @moduledoc """
  Raft Server
  """

  use GenServer

  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

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
      doc: "Statemachine implementation of `ExRaft.Statemachine`"
    ],
    tick_delta: [
      type: :non_neg_integer,
      default: 100,
      doc: "Local tick delta in milliseconds, default 100ms"
    ],
    election_timeout: [
      type: :non_neg_integer,
      doc:
        "Election timeout threshold is a multiple of local_tick. For example, if local_tick is 10ms and I want to initiate a new election every 100ms, this value should be set to 10.",
      default: 20
    ],
    heartbeat_timeout: [
      type: :non_neg_integer,
      doc:
        "Heartbeat timeout threshold is a multiple of local_tick. For example, if local_tick is 10ms and I want to initiate a heartbeat every 100ms, this value should be set to 10.",
      default: 2
    ],
    data_path: [
      type: :string,
      doc: "Data directory, this path contains logs and snapshots",
      default: "./raft_data"
    ],
    snapshot_threshold: [
      type: :non_neg_integer,
      doc: "Snapshot threshold, when log size reachs this value, a snapshot will be taken",
      default: 1000
    ]
  ]

  @type server_opts_t :: [unquote(NimbleOptions.option_typespec(@server_opts_schema))]

  # Client

  @spec start_link(server_opts_t()) :: GenServer.on_start()
  def start_link(opts) do
    opts = NimbleOptions.validate!(opts, @server_opts_schema)

    opts =
      opts
      |> Keyword.put_new_lazy(:remote_impl, fn -> ExRaft.Remote.Erlang.new() end)
      |> Keyword.put_new_lazy(:log_store_impl, fn ->
        ExRaft.LogStore.Cub.new(data_dir: Path.join([opts[:data_path], "#{opts[:id]}", "logs"]))
      end)
      |> Keyword.put_new_lazy(:statemachine_impl, fn -> ExRaft.Mock.Statemachine.new() end)

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

  @spec save_snapshot() :: :ok
  def save_snapshot, do: GenServer.cast(__MODULE__, :save_snapshot)

  @spec read_index() :: {:ok, :committed | :applied | :uncommitted | :timeout} | {:error, any()}
  def read_index do
    GenServer.call(__MODULE__, :read_index)
  end

  # Server (callbacks)

  @impl true
  def init(opts) do
    {:ok, replica_pid} = ExRaft.Replica.start_link(opts)
    {:ok, task_supervisor} = Task.Supervisor.start_link(name: ExRaft.TaskSupervisor)

    {:ok, %{replica_pid: replica_pid, req_waiter: %{}, task_supervisor: task_supervisor}}
  end

  @impl true
  def terminate(reason, _state) do
    Logger.warning("Raft server terminating for #{inspect(reason)}")
  end

  @impl true
  def handle_call(:show_cluster_info, _from, state) do
    %{replica_pid: replica_pid} = state
    {:reply, :gen_statem.call(replica_pid, :show, 1000), state}
  end

  def handle_call(:read_index, from, state) do
    %{replica_pid: replica_pid, task_supervisor: task_supervisor, req_waiter: req_waiter} = state

    task =
      Task.Supervisor.async_nolink(task_supervisor, fn ->
        replica_pid
        |> :gen_statem.send_request(:read_index)
        |> :gen_statem.wait_response(2000)
      end)

    {:noreply, %{state | req_waiter: Map.put(req_waiter, task.ref, from)}}
  end

  def handle_cast(:save_snapshot, state) do
    %{replica_pid: replica_pid} = state
    :gen_statem.cast(replica_pid, :save_snapshot)
    {:noreply, state}
  end

  def handle_cast({:propose, cmds}, state) do
    %{replica_pid: replica_pid} = state
    entries = Enum.map(cmds, fn cmd -> %Pb.Entry{cmd: cmd} end)
    :gen_statem.cast(replica_pid, {:propose, entries})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:pipeout, body}, state) do
    %{replica_pid: replica_pid} = state

    body
    |> ExRaft.Serialize.batch_decode(ExRaft.Pb.Message)
    |> Enum.each(fn msg ->
      :gen_statem.cast(replica_pid, {:pipein, msg})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({ref, answer}, state) do
    %{req_waiter: req_waiter} = state

    {from, req_waiter} = Map.pop(req_waiter, ref)

    from
    |> is_nil()
    |> unless do
      {:reply, reply} = answer
      GenServer.reply(from, reply)
    end

    Process.demonitor(ref, [:flush])
    # Do something with the result and then return
    {:noreply, %{state | req_waiter: req_waiter}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _}, state) do
    %{req_waiter: req_waiter} = state
    {:noreply, %{state | req_waiter: Map.drop(req_waiter, [ref])}}
  end
end
