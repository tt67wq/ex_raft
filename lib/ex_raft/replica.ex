defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.Core
  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Remote
  alias ExRaft.Statemachine
  alias ExRaft.Utils

  require Logger

  # alias ExRaft.Models

  @type state_t :: :follower | :candidate | :leader
  @type term_t :: non_neg_integer()

  @impl true
  def callback_mode do
    [:state_functions, :state_enter]
  end

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, [])
  end

  @impl true
  def init(opts) do
    :rand.seed(:exsss, {100, 101, 102})
    # start log store
    {:ok, _} = LogStore.start_link(opts[:log_store_impl])

    # start statemachine
    {:ok, _} = Statemachine.start_link(opts[:statemachine_impl])

    # start remote client
    {:ok, _} = Remote.start_link(opts[:remote_impl])

    # request register
    {:ok, rr} = Utils.ReqRegister.start_link()

    # fetch last_index & term
    {:ok, last_index} = LogStore.get_last_index(opts[:log_store_impl])
    {:ok, term} = LogStore.get_log_term(opts[:log_store_impl], last_index)

    # bootstrap nodes, NOTE: if snapshot is enabled, try recover remotes from snapshot
    remotes =
      Map.new(opts[:peers], fn
        {id, host, port} -> {id, %Models.Replica{id: id, host: host, port: port}}
      end)

    last_index =
      if last_index == 0 do
        {:ok, cnt} = LogStore.append_log_entries(opts[:log_store_impl], make_new_remote_logs(remotes))
        cnt
      else
        last_index
      end

    {self, _} = remotes |> Map.fetch!(opts[:id]) |> Models.Replica.try_update(last_index)

    {:ok, task_supervisor} = Task.Supervisor.start_link(name: ExRaft.TaskSupervisor)

    state =
      %ReplicaState{
        self: opts[:id],
        remotes: remotes,
        members_count: Enum.count(remotes),
        tick_delta: opts[:tick_delta],
        election_timeout: opts[:election_timeout],
        heartbeat_timeout: opts[:heartbeat_timeout],
        remote_impl: opts[:remote_impl],
        log_store_impl: opts[:log_store_impl],
        statemachine_impl: opts[:statemachine_impl],
        last_index: last_index,
        term: term,
        req_register: rr,
        data_path: opts[:data_path],
        snapshot_threshold: opts[:snapshot_threshold],
        task_supervisor: task_supervisor
      }
      |> Common.update_remote(self)
      |> Common.became_follower(0, 0)
      |> Common.connect_all_remotes()

    {:ok, :follower, state, Common.tick_action(state)}
  end

  # @impl true
  # def terminate(
  #       reason,
  #       current_state,
  #       %Models.ReplicaState{
  #         remote_impl: remote_impl,
  #         log_store_impl: log_store_impl,
  #         statemachine_impl: statemachine_impl
  #       } = state
  #     ) do
  #   Logger.warning(
  #     "terminate: reason: #{inspect(reason)}, current_role: #{inspect(current_state)}",
  #     ReplicaState.metadata(state)
  #   )

  #   Remote.stop(remote_impl)
  #   LogStore.stop(log_store_impl)
  #   Statemachine.stop(statemachine_impl)
  # end

  defdelegate follower(event, data, state), to: Core.Follower

  defdelegate prevote(event, data, state), to: Core.Prevote

  defdelegate candidate(event, data, state), to: Core.Candidate

  defdelegate leader(event, data, state), to: Core.Leader

  defdelegate free(event, data, state), to: Core.Free

  defp make_new_remote_logs(remotes) do
    Enum.with_index(remotes, fn {_, %Models.Replica{id: id, host: host}}, idx ->
      cc = %Pb.ConfigChange{type: :cctype_add_node, replica_id: id, addr: host}

      %Pb.Entry{
        term: 1,
        type: :etype_config_change,
        index: idx + 1,
        cmd: Pb.ConfigChange.encode(cc)
      }
    end)
  end
end
