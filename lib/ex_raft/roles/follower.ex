defmodule ExRaft.Roles.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Pipeline
  alias ExRaft.Roles.Common
  alias ExRaft.Statemachine

  def follower(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :election}, 300, nil}]}
  end

  def follower(
        {:timeout, :election},
        _,
        %Models.ReplicaState{
          election_reset_ts: election_reset_ts,
          election_timeout: election_timeout,
          election_check_delta: election_check_delta
        } = state
      ) do
    if System.system_time(:millisecond) - election_reset_ts > election_timeout do
      {:next_state, :candidate, state}
    else
      {:keep_state_and_data, [{{:timeout, :election}, election_check_delta, nil}]}
    end
  end

  def follower(
        :cast,
        {:pipein, from_id,
         %Models.PackageMaterial{category: Models.RequestVote.Req, data: %Models.RequestVote.Req{} = data}},
        %Models.ReplicaState{} = state
      ) do
    handle_request_vote(from_id, data, state)
  end

  def follower(
        :cast,
        {:pipein, from_id,
         %Models.PackageMaterial{category: Models.AppendEntries.Req, data: %Models.AppendEntries.Req{} = data}},
        %Models.ReplicaState{} = state
      ) do
    handle_append_entries(from_id, data, state)
  end

  def follower(
        :internal,
        :commit,
        %Models.ReplicaState{
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

    {:keep_state, %Models.ReplicaState{state | last_applied: commit_index}}
  end

  def follower(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ------------ Private Functions ------------

  @spec handle_request_vote(
          from_id :: non_neg_integer(),
          req :: Models.RequestVote.Req.t(),
          state :: State.t()
        ) :: any()
  defp handle_request_vote(
         from_id,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{term: current_term, pipeline_impl: pipeline_impl} = state
       )
       when term > current_term do
    # reply
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: term, vote_granted: true}
      }
    ])

    {:keep_state,
     %Models.ReplicaState{state | term: term, voted_for: cid, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_request_vote(
         from_id,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{term: current_term, voted_for: voted_for, pipeline_impl: pipeline_impl} = state
       )
       when term == current_term and voted_for in [-1, cid] do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: term, vote_granted: true}
      }
    ])

    {:keep_state, %Models.ReplicaState{state | voted_for: cid, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_request_vote(from_id, _req, %Models.ReplicaState{term: current_term, pipeline_impl: pipeline_impl}) do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: current_term, vote_granted: false}
      }
    ])

    :keep_state_and_data
  end

  @spec handle_append_entries(
          from_id :: non_neg_integer(),
          req :: Models.AppendEntries.Req.t(),
          state :: State.t()
        ) :: any()

  defp handle_append_entries(
         from_id,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{term: current_term, pipeline_impl: pipeline_impl, last_log_index: last_index} = state
       )
       when term > current_term do
    {cnt, commit_index, commit?, success?} = Common.do_append_entries(req, state)

    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.AppendEntries.Reply,
        data: %Models.AppendEntries.Reply{success: success?, term: current_term, log_index: last_index + cnt}
      }
    ])

    {:keep_state,
     %Models.ReplicaState{
       state
       | voted_for: -1,
         term: term,
         leader_id: leader_id,
         election_reset_ts: System.system_time(:millisecond),
         last_log_index: last_index + cnt,
         commit_index: commit_index
     }, commit_action(commit?)}
  end

  defp handle_append_entries(
         from_id,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{pipeline_impl: pipeline_impl, term: current_term, last_log_index: last_index} = state
       )
       when term == current_term do
    {cnt, commit_index, commit?, success?} = Common.do_append_entries(req, state)

    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.AppendEntries.Reply,
        data: %Models.AppendEntries.Reply{
          success: success?,
          term: current_term,
          log_index: last_index + cnt
        }
      }
    ])

    {:keep_state,
     %Models.ReplicaState{
       state
       | last_log_index: last_index + cnt,
         commit_index: commit_index,
         leader_id: leader_id,
         election_reset_ts: System.system_time(:millisecond)
     }, commit_action(commit?)}
  end

  # term mismatch
  defp handle_append_entries(from_id, _req, %Models.ReplicaState{
         pipeline_impl: pipeline_impl,
         term: current_term,
         last_log_index: last_index
       }) do
    Pipeline.pipeout(pipeline_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.AppendEntries.Reply,
        data: %Models.AppendEntries.Reply{
          term: current_term,
          success: false,
          log_index: last_index
        }
      }
    ])

    :keep_state_and_data
  end

  defp commit_action(true), do: [{:next_event, :internal, :commit}]
  defp commit_action(false), do: []
end
