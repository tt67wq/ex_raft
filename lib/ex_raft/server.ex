defmodule ExRaft.Server do
  @moduledoc """
  Raft Server
  """

  use GenServer

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
    pipeline_impl: [
      type: :any,
      doc: "Implementation of `ExRaft.Pipeline`"
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
      |> Keyword.put_new(:pipeline_impl, ExRaft.Pipeline.Erlang.new())
      |> Keyword.put_new(:log_store_impl, ExRaft.LogStore.Inmem.new())
      |> NimbleOptions.validate!(@server_opts_schema)

    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
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
  def handle_call(:show_cluster_info, _from, %{replica_pid: replica_pid} = state) do
    {:reply, :gen_statem.call(replica_pid, :show), state}
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
