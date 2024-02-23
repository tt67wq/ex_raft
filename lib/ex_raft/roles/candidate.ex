defmodule ExRaft.Roles.Candidate do
  @moduledoc """
  Candidate Role Module

  Handle :gen_statm callbacks for candidate role
  """

  alias ExRaft.Exception
  alias ExRaft.Models
  alias ExRaft.Pipeline
  alias ExRaft.Roles.Common
  alias ExRaft.Rpc

  def candidate(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :election}, 300, nil}]}
  end

  def candidate(
        {:timeout, :election},
        _,
        %Models.ReplicaState{
          election_reset_ts: election_reset_ts,
          election_timeout: election_timeout,
          election_check_delta: election_check_delta
        } = state
      ) do
    if System.system_time(:millisecond) - election_reset_ts > election_timeout do
      run_election(state)
    else
      {:keep_state_and_data, [{{:timeout, :election}, election_check_delta, nil}]}
    end
  end

  def candidate(
        :cast,
        {:pipein, from_id,
         %Models.PackageMaterial{category: Models.RequestVote.Req, data: %Models.RequestVote.Req{} = data}},
        %Models.ReplicaState{} = state
      ) do
    handle_request_vote(from_id, data, state)
  end

  def candidate(
        :cast,
        {:pipein, from_id,
         %Models.PackageMaterial{category: Models.AppendEntries.Req, data: %Models.AppendEntries.Req{} = data}},
        %Models.ReplicaState{} = state
      ) do
    handle_append_entries(from_id, data, state)
  end

  def candidate(
        :cast,
        {:pipein, from_id,
         %Models.PackageMaterial{category: Models.RequestVote.Reply, data: %Models.RequestVote.Reply{} = data}},
        state
      ) do
    # handle_request_vote_reply(from_id, data, state)
  end

  def candidate(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ------- private functions -------

  defp run_election(%Models.ReplicaState{term: term, peers: [], self: %Models.Replica{id: id}} = state) do
    # no other peers, become leader
    {:next_state, :leader, %Models.ReplicaState{state | term: term + 1, voted_for: id}}
  end

  defp run_election(
         %Models.ReplicaState{term: term, peers: peers, self: %Models.Replica{id: id}, rpc_impl: rpc_impl} = state
       ) do
    term = term + 1

    ms =
      Enum.map(
        peers,
        fn %Models.Replica{id: to_id} ->
          {to_id,
           [
             %Models.PackageMaterial{
               category: Models.RequestVote.Req,
               data: %Models.RequestVote.Req{term: term, candidate_id: id}
             }
           ]}
        end
      )

    Pipeline.batch_pipeout(rpc_impl, ms)

    {:keep_state,
     %Models.ReplicaState{state | term: term, voted_for: id, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_append_entries(
         from,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{term: current_term, last_log_index: last_index} = state
       )
       when term >= current_term do
    {cnt, commit_index, commit?} = Common.do_append_entries(req, state)

    {:next_state, :follower,
     %Models.ReplicaState{
       state
       | term: term,
         election_reset_ts: System.system_time(:millisecond),
         voted_for: -1,
         leader_id: leader_id,
         last_log_index: last_index + cnt,
         commit_index: commit_index
     }, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: cnt > 0}}} | commit_action(commit?)]}
  end

  # term mismatch
  defp handle_append_entries(from, _req, %Models.ReplicaState{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp handle_request_vote(
         from_id,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{rpc_impl: rpc_impl, term: current_term} = state
       )
       when term > current_term do
    Pipeline.pipeout(rpc_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: term, vote_granted: true}
      }
    ])

    {:next_state, :follower,
     %Models.ReplicaState{state | term: term, voted_for: cid, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_request_vote(
         from_id,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{rpc_impl: rpc_impl, term: current_term, voted_for: voted_for} = state
       )
       when current_term == term and voted_for in [-1, cid] do
    Pipeline.pipeout(rpc_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: current_term, vote_granted: true}
      }
    ])

    {:next_state, :follower,
     %Models.ReplicaState{state | voted_for: cid, election_reset_ts: System.system_time(:millisecond)}}
  end

  defp handle_request_vote(from_id, _req, %Models.ReplicaState{rpc_impl: rpc_impl, term: current_term}) do
    Pipeline.pipeout(rpc_impl, from_id, [
      %Models.PackageMaterial{
        category: Models.RequestVote.Reply,
        data: %Models.RequestVote.Reply{term: current_term, vote_granted: false}
      }
    ])

    :keep_state_and_data
  end

  defp commit_action(true), do: [{:next_event, :interal, :commit}]
  defp commit_action(false), do: []
end
