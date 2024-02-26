defmodule ExRaft.Roles.Leader do
  @moduledoc """
  Leader Role Module

  Handle :gen_statm callbacks for leader role
  """
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pipeline
  alias ExRaft.Roles.Common
  alias ExRaft.Statemachine

  require Logger

  def leader(
        :enter,
        _old_state,
        %ReplicaState{self: %Models.Replica{id: id}, peers: peers, last_log_index: last_log_index} = state
      ) do
    Logger.info("I become leader: #{id}")

    # init match_index
    match_index = Enum.reduce(peers, %{}, fn %Models.Replica{id: pid}, acc -> Map.put(acc, pid, last_log_index) end)

    {
      :keep_state,
      %ReplicaState{state | match_index: match_index},
      [{{:timeout, :heartbeat}, 50, nil}]
    }
  end

  def leader({:timeout, :heartbeat}, _, %ReplicaState{
        self: %Models.Replica{id: id},
        peers: peers,
        term: current_term,
        pipeline_impl: pipeline_impl,
        log_store_impl: log_store_impl,
        heartbeat_delta: heartbeat_delta,
        match_index: match_index,
        last_log_index: last_log_index,
        commit_index: commit_index
      }) do
    ms =
      Enum.map(peers, fn %Models.Replica{id: to_id} ->
        prev_index = Map.fetch!(match_index, to_id)
        prev_term = (prev_index == -1 && -1) || LogStore.get_log_term(log_store_impl, prev_index)
        {:ok, entries} = LogStore.get_range(log_store_impl, prev_index, last_log_index)

        {to_id,
         [
           %Models.PackageMaterial{
             category: Models.AppendEntries.Req,
             data: %Models.AppendEntries.Req{
               term: current_term,
               leader_id: id,
               prev_log_index: prev_index,
               prev_log_term: prev_term,
               entries: entries,
               leader_commit: commit_index
             }
           }
         ]}
      end)

    Pipeline.batch_pipeout(pipeline_impl, ms)
    {:keep_state_and_data, [{{:timeout, :heartbeat}, heartbeat_delta, nil}]}
  end

  def leader(
        :cast,
        {:pipeint, from_id,
         %Models.PackageMaterial{category: Models.RequestVote.Req, data: %Models.RequestVote.Req{} = req}},
        %ReplicaState{} = state
      ) do
    handle_request_vote(from_id, req, state)
  end

  def leader(
        :cast,
        {:pipeint, from_id,
         %Models.PackageMaterial{category: Models.AppendEntries.Req, data: %Models.AppendEntries.Req{} = req}},
        %ReplicaState{} = state
      ) do
    handle_append_entries(from_id, req, state)
  end

  def leader(
        :cast,
        {:pipeint, from_id,
         %Models.PackageMaterial{category: Models.AppendEntries.Reply, data: %Models.AppendEntries.Reply{} = req}},
        %ReplicaState{} = state
      ) do
    handle_append_entries_reply(from_id, req, state)
  end

  def leader(
        :internal,
        :commit,
        %ReplicaState{
          statemachine_impl: statemachine_impl,
          log_store_impl: log_store_impl,
          commit_index: commit_index,
          last_applied: last_applied
        } = state
      ) do
    {:ok, logs} = LogStore.get_range(log_store_impl, last_applied, commit_index)

    cmds =
      logs
      |> Enum.with_index(last_applied + 1)
      |> Enum.map(fn {index, %Models.LogEntry{command: cmd}} -> %Models.CommandEntry{index: index, command: cmd} end)

    :ok = Statemachine.handle_commands(statemachine_impl, cmds)

    {:keep_state, %ReplicaState{state | last_applied: commit_index}}
  end

  def leader(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ------- private functions -------

  @spec handle_request_vote(
          from_id :: non_neg_integer(),
          req :: Models.RequestVote.Req.t(),
          state :: ReplicaState.t()
        ) :: any()
  defp handle_request_vote(from_id, %Models.RequestVote.Req{term: term}, %ReplicaState{
         pipeline_impl: pipeline_impl,
         term: current_term
       })
       when term > current_term do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: term, vote_granted: true}
      }
    ])

    {:next_state, :follower,
     %ReplicaState{term: term, voted_for: -1, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_request_vote(from_id, _req, %ReplicaState{pipeline_impl: pipeline_impl, term: current_term}) do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: current_term, vote_granted: false}
      }
    ])

    :keep_state_and_data
  end

  defp handle_append_entries(
         from_id,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %ReplicaState{term: current_term, last_log_index: last_index, pipeline_impl: pipeline_impl} = state
       )
       when term > current_term do
    {cnt, commit_index, commit?, success?} = Common.do_append_entries(req, state)

    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.AppendEntries.Reply,
        data: %Models.AppendEntries.Reply{success: success?, term: current_term, log_index: last_index + cnt}
      }
    ])

    {:next_state, :follower,
     %ReplicaState{
       term: term,
       voted_for: -1,
       leader_id: leader_id,
       election_reset_ts: System.system_time(:millisecond),
       last_log_index: last_index + cnt,
       commit_index: commit_index
     }, commit_action(commit?)}
  end

  # term mismatch
  defp handle_append_entries(from_id, _req, %ReplicaState{
         term: current_term,
         pipeline_impl: pipeline_impl,
         last_log_index: last_index
       }) do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.AppendEntries.Reply,
        data: %Models.AppendEntries.Reply{success: false, term: current_term, log_index: last_index}
      }
    ])

    :keep_state_and_data
  end

  defp handle_append_entries_reply(
         _from_id,
         %Models.AppendEntries.Reply{term: term},
         %ReplicaState{term: current_term} = state
       )
       when term > current_term do
    {:next_state, :follower,
     %ReplicaState{state | term: term, election_reset_ts: System.system_time(:millisecond), voted_for: -1}}
  end

  defp handle_append_entries_reply(
         from_id,
         %Models.AppendEntries.Reply{success: true, term: term, log_index: log_index},
         %ReplicaState{
           term: current_term,
           last_log_index: last_index,
           last_applied: last_applied,
           match_index: match_index,
           commit_index: commit_index,
           peers: peers
         } = state
       )
       when current_term == term do
    match_index = Map.put(match_index, from_id, log_index)

    commit_index =
      Enum.find(last_index..(commit_index + 1), fn index ->
        index_matched?(index, match_index, Enum.count(peers) + 1)
      end)

    {:keep_state, %ReplicaState{state | match_index: match_index, commit_index: commit_index},
     commit_action(commit_index > last_applied)}
  end

  defp index_matched?(index, match_index, replica_cnt) do
    match_index
    |> Map.values()
    |> Enum.count(fn x -> x >= index end)
    |> Kernel.*(2)
    |> Kernel.>(replica_cnt)
  end

  defp commit_action(true), do: [{:next_event, :internal, :commit}]
  defp commit_action(false), do: []
end
