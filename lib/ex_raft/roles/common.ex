defmodule ExRaft.Roles.Common do
  @moduledoc """
  Common behavior for all roles
  """
  alias ExRaft.Config
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Pipeline
  alias ExRaft.Statemachine
  alias ExRaft.Typespecs

  require Logger

  def can_vote?(%Pb.Message{from: cid, term: term}, %ReplicaState{term: current_term, voted_for: voted_for})
      when voted_for in [cid, 0] or term > current_term do
    true
  end

  def can_vote?(_, _), do: false

  def log_updated?(_req, nil), do: true

  def log_updated?(%Pb.Message{log_term: req_last_log_term}, %Pb.Entry{term: last_log_term})
      when req_last_log_term > last_log_term,
      do: true

  def log_updated?(%Pb.Message{log_index: req_last_log_index, log_term: req_last_log_term}, %Pb.Entry{
        term: last_log_term,
        index: last_log_index
      })
      when req_last_log_term == last_log_term and req_last_log_index >= last_log_index,
      do: true

  def log_updated?(_, _), do: false

  def cast_action(msg), do: [{:next_event, :cast, {:pipein, msg}}]

  @spec handle_term_mismatch(
          follower? :: boolean(),
          msg :: Typespecs.message_t(),
          state :: ReplicaState.t()
        ) :: any()
  def handle_term_mismatch(_follower?, %Pb.Message{term: term}, %ReplicaState{
        term: current_term,
        leader_id: leader_id,
        election_tick: election_tick,
        election_timeout: election_timeout
      })
      when term > current_term and leader_id != 0 and election_tick < election_timeout do
    # we got a RequestVote with higher term, but we recently had heartbeat msg
    # from leader within the minimum election timeout and that leader is known
    # to have quorum. we thus drop such RequestVote to minimize interruption by
    # network partitioned nodes with higher term.
    # this idea is from the last paragraph of the section 6 of the raft paper
    :keep_state_and_data
  end

  def handle_term_mismatch(
        follower?,
        %Pb.Message{type: :requst_vote, term: term} = msg,
        %ReplicaState{term: current_term} = state
      )
      when term > current_term do
    # not to reset the electionTick value to avoid the risk of having the
    # local node not being to campaign at all. if the local node generates
    # the tick much slower than other nodes (e.g. bad config, hardware
    # clock issue, bad scheduling, overloaded etc), it may lose the chance
    # to ever start a campaign unless we keep its electionTick value here.
    if follower? do
      {:keep_state, became_follower_keep_election(state, term, 0), cast_action(msg)}
    else
      {:next_state, :follower, became_follower_keep_election(state, term, 0), cast_action(msg)}
    end
  end

  def handle_term_mismatch(
        follower?,
        %Pb.Message{from: from_id, term: term} = msg,
        %ReplicaState{term: current_term} = state
      )
      when term > current_term do
    leader_id = (leader_message?(msg) && from_id) || 0

    if follower? do
      {:keep_state, became_follower(state, term, leader_id), cast_action(msg)}
    else
      {:next_state, :follower, became_follower(state, term, leader_id), cast_action(msg)}
    end
  end

  def handle_term_mismatch(_, _, _, _), do: :keep_state_and_data

  defp gen_election_timeout(timeout), do: Enum.random(timeout..(2 * timeout))

  @spec reset(state :: ReplicaState.t(), term :: Typespecs.term_t(), leader? :: boolean()) :: ReplicaState.t()
  def reset(state, term, false) do
    state
    |> set_term(term)
    |> reset_election_tick()
  end

  def reset(%ReplicaState{} = state, term, true) do
    state
    |> set_term(term)
    |> reset_hearbeat_tick()
  end

  defp set_term(%ReplicaState{term: current_term} = state, term) when term != current_term do
    %ReplicaState{state | term: term, voted_for: 0}
  end

  defp set_term(state, _term), do: state

  defp reset_election_tick(%ReplicaState{election_timeout: election_timeout} = state) do
    %ReplicaState{state | election_tick: 0, randomized_election_timeout: gen_election_timeout(election_timeout)}
  end

  defp reset_hearbeat_tick(%ReplicaState{} = state), do: %ReplicaState{state | heartbeat_tick: 0}

  @spec tick(state :: ReplicaState.t(), leader? :: bool()) :: ReplicaState.t()
  def tick(%ReplicaState{local_tick: local_tick, election_tick: election_tick, apply_tick: apply_tick} = state, false) do
    %ReplicaState{
      state
      | local_tick: local_tick + 1,
        apply_tick: apply_tick + 1,
        election_tick: election_tick + 1
    }
  end

  def tick(%ReplicaState{local_tick: local_tick, heartbeat_tick: heartbeat_tick, apply_tick: apply_tick} = state, true) do
    %ReplicaState{
      state
      | local_tick: local_tick + 1,
        apply_tick: apply_tick + 1,
        heartbeat_tick: heartbeat_tick + 1
    }
  end

  def tick_action(%ReplicaState{tick_delta: tick_delta}), do: [{{:timeout, :tick}, tick_delta, nil}]

  def campaign(%ReplicaState{term: term, self: id} = state) do
    state
    |> reset(term + 1, false)
    |> set_leader_id(id)
    |> reset_votes()
    |> tick(false)
  end

  defp reset_votes(%ReplicaState{self: id} = state) do
    state
    |> Map.put(:votes, %{id => false})
    |> vote_for(id)
  end

  defp set_leader_id(%ReplicaState{} = state, id) do
    %ReplicaState{state | leader_id: id}
  end

  def became_leader(%ReplicaState{term: term} = state), do: became_leader(state, term)

  def became_leader(%ReplicaState{self: id} = state, term) do
    state
    |> reset(term, true)
    |> set_leader_id(id)
  end

  def became_candidate(%ReplicaState{term: term} = state) do
    state
    |> reset(term, false)
    |> set_leader_id(0)
  end

  def became_follower(%ReplicaState{} = state, term, leader_id) do
    state
    |> reset(term, false)
    |> set_leader_id(leader_id)
  end

  def became_follower_keep_election(%ReplicaState{} = state, term, leader_id) do
    state
    |> set_term(term)
    |> set_leader_id(leader_id)
  end

  @spec vote_for(state :: ReplicaState.t(), vote_for_id :: Typespecs.replica_id_t()) :: ReplicaState.t()
  def vote_for(state, vote_for_id) do
    %ReplicaState{state | voted_for: vote_for_id}
  end

  defp leader_message?(%Pb.Message{type: type}) do
    type in [:append_entries, :heartbeat]
  end

  def vote_quorum_pass?(%ReplicaState{votes: votes, members_count: members_count}) do
    votes
    |> Enum.count(fn {_, rejected?} -> not rejected? end)
    |> Kernel.*(2)
    |> Kernel.>(members_count)
  end

  @spec send_msgs(ReplicaState.t(), [Typespecs.message_t()]) :: :ok | {:error, ExRaft.Exception.t()}
  def send_msgs(%ReplicaState{pipeline_impl: pipeline_impl}, msgs), do: Pipeline.pipeout(pipeline_impl, msgs)

  def send_msg(%ReplicaState{} = state, %Pb.Message{} = msg) do
    send_msgs(state, [msg])
  end

  defp apply_index(%ReplicaState{last_applied: last_applied} = state, index) when last_applied == index do
    %ReplicaState{state | apply_tick: 0}
  end

  defp apply_index(%ReplicaState{last_applied: last_applied} = state, index) when last_applied < index do
    %ReplicaState{state | last_applied: index, apply_tick: 0}
  end

  @spec apply_to_statemachine(ReplicaState.t()) :: ReplicaState.t()
  def apply_to_statemachine(%ReplicaState{commit_index: commit_index, last_applied: last_applied} = state)
      when commit_index == last_applied,
      do: %ReplicaState{state | apply_tick: 0}

  def apply_to_statemachine(
        %ReplicaState{
          commit_index: commit_index,
          last_applied: last_applied,
          log_store_impl: log_store_impl,
          statemachine_impl: statemachine_impl
        } = state
      )
      when commit_index > last_applied do
    max_limit = Config.max_msg_batch_size()
    limit = (commit_index - last_applied > max_limit && max_limit) || commit_index - last_applied

    log_store_impl
    |> LogStore.get_limit(last_applied, limit)
    |> case do
      {:ok, []} ->
        %ReplicaState{state | apply_tick: 0}

      {:ok, logs} ->
        :ok = Statemachine.handle_commands(statemachine_impl, logs)
        %Pb.Entry{index: index} = List.last(logs)

        apply_index(state, index)
    end
  end

  def apply_to_statemachine(_, state), do: state

  @spec local_peer(ReplicaState.t()) :: Models.Replica.t()
  def local_peer(%ReplicaState{remotes: remotes, self: id}) do
    %{^id => peer} = remotes
    peer
  end

  @spec update_remote(ReplicaState.t(), Models.Replica.t()) :: ReplicaState.t()
  def update_remote(%ReplicaState{remotes: remotes} = state, %Models.Replica{id: id} = peer) do
    %ReplicaState{state | remotes: Map.put(remotes, id, peer)}
  end

  def quorum(%ReplicaState{members_count: members_count}) do
    members_count
    |> div(2)
    |> Kernel.+(1)
  end
end
