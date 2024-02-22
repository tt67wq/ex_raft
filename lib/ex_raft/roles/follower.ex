defmodule ExRaft.Roles.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  alias ExRaft.Exception
  alias ExRaft.LogStore
  alias ExRaft.Models
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
        {:call, from},
        {:rpc_call, %Models.RequestVote.Req{candidate_id: candidate_id} = req},
        %Models.ReplicaState{peers: peers} = state
      ) do
    # Logger.debug("follower: handle request vote: term: #{term}, candidate_id: #{candidate_id}")
    requst_peer = find_peer(candidate_id, peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(from, req, state)
    end
  end

  def follower(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{leader_id: leader_id} = req},
        %Models.ReplicaState{peers: peers} = state
      ) do
    # Logger.debug("follower: handle append entries: term: #{term}, leader_id: #{leader_id}")
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(from, req, state)
    end
  end

  def follower({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :follower}}}]}
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

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)

  @spec handle_request_vote(
          from :: pid(),
          req :: Models.RequestVote.Req.t(),
          state :: State.t()
        ) :: any()
  defp handle_request_vote(
         from,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{term: current_term} = state
       )
       when term > current_term do
    {:keep_state,
     %Models.ReplicaState{state | term: term, voted_for: cid, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(
         from,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %Models.ReplicaState{term: current_term, voted_for: voted_for} = state
       )
       when term == current_term and voted_for in [-1, cid] do
    {:keep_state, %Models.ReplicaState{state | voted_for: cid, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(from, _req, %Models.ReplicaState{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  @spec handle_append_entries(
          from :: pid(),
          req :: Models.AppendEntries.Req.t(),
          state :: State.t()
        ) :: any()

  defp handle_append_entries(
         from,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{term: current_term, last_log_index: last_index} = state
       )
       when term > current_term do
    {cnt, commit_index, commit?} = Common.do_append_entries(req, state)

    {:keep_state,
     %Models.ReplicaState{
       state
       | voted_for: -1,
         term: term,
         leader_id: leader_id,
         election_reset_ts: System.system_time(:millisecond),
         last_log_index: last_index + cnt,
         commit_index: commit_index
     }, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: cnt > 0}}} | commit_action(commit?)]}
  end

  defp handle_append_entries(
         from,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{term: current_term, last_log_index: last_index} = state
       )
       when term == current_term do
    {cnt, commit_index, commit?} = Common.do_append_entries(req, state)

    {:keep_state,
     %Models.ReplicaState{
       state
       | last_log_index: last_index + cnt,
         commit_index: commit_index,
         leader_id: leader_id,
         election_reset_ts: System.system_time(:millisecond)
     },
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: cnt > 0}}} | commit_action(commit?)]}
  end

  # term mismatch
  defp handle_append_entries(from, _req, %Models.ReplicaState{term: current_term}) do
    {:keep_data_and_state, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp commit_action(true), do: [{:next_event, :interal, :commit}]
  defp commit_action(false), do: []
end
