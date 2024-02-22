defmodule ExRaft.Roles.Leader do
  @moduledoc """
  Leader Role Module

  Handle :gen_statm callbacks for leader role
  """
  alias ExRaft.Exception
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Roles.Common
  alias ExRaft.Rpc
  alias ExRaft.Statemachine

  def leader(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :heartbeat}, 50, nil}]}
  end

  def leader(
        {:timeout, :heartbeat},
        _,
        %Models.ReplicaState{
          self: %Models.Replica{id: id},
          peers: peers,
          term: current_term,
          rpc_impl: rpc_impl,
          heartbeat_delta: heartbeat_delta
        } = state
      ) do
    peers
    |> Enum.map(fn peer ->
      Task.async(fn ->
        {peer, Rpc.just_call(rpc_impl, peer, %Models.AppendEntries.Req{term: current_term, leader_id: id})}
      end)
    end)
    |> Task.await_many(200)
    |> Enum.reduce_while(0, fn
      %Models.AppendEntries.Reply{
        term: term
      },
      _acc
      when term > current_term ->
        {:halt, term}

      {_, _}, acc ->
        {:cont, acc}
    end)
    |> case do
      0 ->
        {:keep_state_and_data, [{{:timeout, :heartbeat}, heartbeat_delta, nil}]}

      term ->
        {:next_state, :follower, %Models.ReplicaState{state | term: term, voted_for: -1}}
    end
  end

  def leader(
        {:call, from},
        {:rpc_call, %Models.RequestVote.Req{term: term, candidate_id: candidate_id}},
        %Models.ReplicaState{peers: peers} = state
      ) do
    requst_peer = find_peer(candidate_id, peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(from, term, state)
    end
  end

  def leader(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{leader_id: leader_id} = req},
        %Models.ReplicaState{peers: peers} = state
      ) do
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(from, req, state)
    end
  end

  def leader({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :leader}}}]}
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

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)

  defp handle_request_vote(from, %Models.RequestVote.Req{candidate_id: cid, term: term}, %Models.ReplicaState{
         term: current_term
       })
       when term > current_term do
    {:next_state, :follower, %Models.ReplicaState{term: term, voted_for: cid},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(from, _req, %Models.ReplicaState{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  defp handle_append_entries(
         from,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %Models.ReplicaState{term: current_term, last_log_index: last_index} = state
       )
       when term > current_term do
    {cnt, commit_index, commit?} = Common.do_append_entries(req, state)

    {:next_state, :follower,
     %Models.ReplicaState{
       term: term,
       voted_for: -1,
       leader_id: leader_id,
       election_reset_ts: System.system_time(:millisecond),
       last_log_index: last_index + cnt,
       commit_index: commit_index
     }, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: cnt > 0}}} | commit_action(commit?)]}
  end

  # term mismatch
  defp handle_append_entries(from, _req, %Models.ReplicaState{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp commit_action(true), do: [{:next_event, :interal, :commit}]
  defp commit_action(false), do: []
end
