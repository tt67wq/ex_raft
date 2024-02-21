defmodule ExRaft.Roles.Candidate do
  @moduledoc """
  Candidate Role Module

  Handle :gen_statm callbacks for candidate role
  """

  alias ExRaft.Exception
  alias ExRaft.Models
  alias ExRaft.Replica.State
  alias ExRaft.Roles.Common
  alias ExRaft.Rpc
  alias ExRaft.LogStore
  alias ExRaft.Statemachine

  def candidate(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :election}, 300, nil}]}
  end

  def candidate(
        {:timeout, :election},
        _,
        %State{
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

  def candidate({:call, from}, {:rpc_call, %Models.RequestVote.Req{term: term, candidate_id: candidate_id}}, state) do
    requst_peer = find_peer(candidate_id, state.peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(from, term, state)
    end
  end

  def candidate(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{leader_id: leader_id} = req},
        %State{peers: peers} = state
      ) do
    # Logger.debug("candidate: handle append entries: term: #{term}, leader_id: #{leader_id}")
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(from, req, state)
    end
  end

  def candidate({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :candidate}}}]}
  end

  def candidate(
        :internal,
        :commit,
        %State{
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

    {:keep_state, %State{state | last_applied: commit_index}}
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

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)

  defp run_election(%State{term: term, peers: [], self: %Models.Replica{id: id}} = state) do
    # no other peers, become leader
    {:next_state, :leader, %State{state | term: term + 1, voted_for: id}}
  end

  defp run_election(
         %State{
           term: term,
           peers: peers,
           self: %Models.Replica{id: id},
           rpc_impl: rpc_impl,
           election_check_delta: election_check_delta
         } = state
       ) do
    term = term + 1
    # start election
    peers
    |> Enum.map(fn peer ->
      Task.async(fn ->
        Rpc.just_call(rpc_impl, peer, %Models.RequestVote.Req{term: term, candidate_id: id})
      end)
    end)
    |> Task.await_many(2000)
    |> Enum.reduce_while(
      {1, :candidate, term},
      fn
        %Models.RequestVote.Reply{
          term: reply_term
        },
        {votes, _, _}
        when reply_term > term ->
          # a higher term is found, become follower
          {:halt, {votes, :follower, reply_term}}

        %Models.RequestVote.Reply{vote_granted: true, term: ^term}, {votes, :candidate, _} ->
          if 2 * (votes + 1) > Enum.count(peers) + 1 do
            # over half of the peers voted for me, become leader
            {:halt, {votes + 1, :leader, term}}
          else
            {:cont, {votes + 1, :candidate, term}}
          end

        _resp, acc ->
          # vote not granted or error, continue
          {:cont, acc}
      end
    )
    # |> ExRaft.Debug.debug()
    |> case do
      # election success
      {_, :leader, _} ->
        {:next_state, :leader, %State{state | term: term, voted_for: id}}

      # higher term found, become follower
      {_, :follower, higher_term} ->
        {:next_state, :follower,
         %State{state | term: higher_term, voted_for: -1, election_reset_ts: System.system_time(:millisecond)}}

      # election failed, restart election
      {_, :candidate, _} ->
        {:keep_state, %State{state | term: term, voted_for: id}, [{{:timeout, :election}, election_check_delta, nil}]}
    end
  end

  defp handle_append_entries(
         from,
         %Models.AppendEntries.Req{term: term, leader_id: leader_id} = req,
         %State{term: current_term, last_log_index: last_index} = state
       )
       when term >= current_term do
    {cnt, commit_index, commit?} = Common.do_append_entries(req, state)

    {:next_state, :follower,
     %State{
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
  defp handle_append_entries(from, _req, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp handle_request_vote(
         from,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %State{term: current_term} = state
       )
       when term > current_term do
    {:next_state, :follower,
     %State{state | term: term, voted_for: cid, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(
         from,
         %Models.RequestVote.Req{candidate_id: cid, term: term},
         %State{term: current_term, voted_for: voted_for} = state
       )
       when current_term == term and voted_for in [-1, cid] do
    {:next_state, :follower, %State{state | voted_for: cid, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: true}}}]}
  end

  defp handle_request_vote(from, _req, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  defp commit_action(true), do: [{:next_event, :interal, :commit}]
  defp commit_action(false), do: []
end
