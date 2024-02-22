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

  require Logger

  def leader(
        :enter,
        _old_state,
        %Models.ReplicaState{self: %Models.Replica{id: id}, peers: peers, last_log_index: last_log_index} = state
      ) do
    Logger.info("I become leader: #{id}")

    # init next_index and match_index
    next_index = Enum.reduce(peers, %{}, fn %Models.Replica{id: pid}, acc -> Map.put(acc, pid, last_log_index + 1) end)
    match_index = Enum.reduce(peers, %{}, fn %Models.Replica{id: pid}, acc -> Map.put(acc, pid, -1) end)

    {
      :keep_state,
      %Models.ReplicaState{state | next_index: next_index, match_index: match_index},
      [{{:timeout, :heartbeat}, 50, nil}]
    }
  end

  def leader(
        {:timeout, :heartbeat},
        _,
        %Models.ReplicaState{
          self: %Models.Replica{id: id},
          peers: peers,
          term: current_term,
          rpc_impl: rpc_impl,
          log_store_impl: log_store_impl,
          heartbeat_delta: heartbeat_delta,
          next_index: next_index,
          last_log_index: last_log_index,
          commit_index: commit_index,
          match_index: match_index
        } = state
      ) do
    peers
    |> Task.async_stream(fn %Models.Replica{id: peer_id} = peer ->
      ni = Map.fetch!(next_index, peer_id)
      prev_index = ni - 1
      prev_term = (prev_index == -1 && -1) || LogStore.get_log_term(log_store_impl, prev_index)
      {:ok, entries} = LogStore.get_range(log_store_impl, prev_index, last_log_index)

      req = %Models.AppendEntries.Req{
        term: current_term,
        leader_id: id,
        prev_log_index: prev_index,
        prev_log_term: prev_term,
        entries: entries,
        leader_commit: commit_index
      }

      {peer, Rpc.just_call(rpc_impl, peer, req), last_log_index - prev_index}
    end)
    |> Enum.reduce_while(
      %{
        success: 0,
        term: current_term,
        next_index: next_index,
        match_index: match_index,
        commit_index: commit_index
      },
      fn
        {:ok, {_, %Models.AppendEntries.Reply{term: term}, _}}, acc
        when term > current_term ->
          {:halt, %{acc | term: term}}

        {:ok, {_, %Models.AppendEntries.Reply{success: true}, 0}}, %{success: success} = acc ->
          {:cont, %{acc | success: success + 1}}

        {:ok,
         {
           %Models.Replica{id: peer_id},
           %Models.AppendEntries.Reply{success: true},
           entries_count
         }},
        %{success: success, next_index: next_index, match_index: match_index, commit_index: commit_index} = acc ->
          %{^peer_id => ni} = next_index = Map.update!(next_index, peer_id, &(&1 + entries_count))
          match_index = Map.put(match_index, peer_id, ni - 1)

          max_commit = max_match_commit_index(last_log_index, commit_index, match_index, id, peers)

          {:cont,
           %{acc | success: success + 1, next_index: next_index, match_index: match_index, commit_index: max_commit}}

        # failed
        _, acc ->
          {:cont, acc}
      end
    )
    |> case do
      %{
        term: term,
        next_index: next_index,
        match_index: match_index,
        commit_index: new_commit_index
      }
      when term > current_term ->
        {:next_state, :follower,
         %Models.ReplicaState{
           state
           | term: term,
             voted_for: -1,
             leader_id: -1,
             election_reset_ts: System.system_time(:millisecond),
             next_index: next_index,
             match_index: match_index,
             commit_index: new_commit_index
         }, commit_action(new_commit_index > commit_index)}

      %{
        next_index: next_index,
        match_index: match_index,
        commit_index: new_commit_index
      } ->
        {:keep_state,
         %Models.ReplicaState{state | next_index: next_index, match_index: match_index, commit_index: new_commit_index},
         [{{:timeout, :heartbeat}, heartbeat_delta, nil} | commit_action(new_commit_index > commit_index)]}
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

  defp max_match_commit_index(last_log_index, commit_index, match_index, self_id, peers) do
    Enum.find(last_log_index..(commit_index + 1), fn i ->
      match_cnt = match_index |> Map.delete(self_id) |> Map.values() |> Enum.count(fn x -> x >= i end)
      match_cnt * 2 > Enum.count(peers) + 1
    end)
  end

  defp commit_action(true), do: [{:next_event, :interal, :commit}]
  defp commit_action(false), do: []
end
