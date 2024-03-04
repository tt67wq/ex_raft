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

  @spec do_append_entries(
          req :: Typespecs.message_t(),
          state :: ReplicaState.t()
        ) :: {
          append_count :: non_neg_integer(),
          commit_index :: Typespecs.index_t(),
          reject? :: boolean
        }
  def do_append_entries(%Pb.Message{log_index: log_index}, %ReplicaState{commit_index: commit_index})
      when log_index < commit_index do
    {0, commit_index, false}
  end

  def do_append_entries(
        %Pb.Message{log_index: log_index, log_term: log_term, entries: entries, commit: leader_commit},
        %ReplicaState{log_store_impl: log_store_impl, commit_index: commit_index, last_log_index: last_index}
      )
      when log_index <= last_index do
    log_index
    |> match_term?(log_term, log_store_impl)
    |> if do
      to_append_entries =
        entries
        |> get_conflict_index(log_store_impl)
        |> case do
          0 -> []
          conflict_index -> Enum.slice(entries, (conflict_index - log_index - 1)..-1//1)
        end

      {:ok, cnt} = LogStore.append_log_entries(log_store_impl, to_append_entries)

      {cnt, min(leader_commit, log_index + cnt), false}
    else
      {0, commit_index, true}
    end
  end

  def do_append_entries(_req, %ReplicaState{commit_index: commit_index}) do
    {0, commit_index, true}
  end

  defp match_term?(log_index, log_term, log_store_impl) do
    log_store_impl
    |> LogStore.get_log_entry(log_index)
    |> case do
      {:ok, nil} -> false
      {:ok, %Pb.Entry{term: tm}} -> tm == log_term
      {:error, e} -> raise e
    end
  end

  defp get_conflict_index(entries, log_store_impl) do
    entries
    |> Enum.find(fn %Pb.Entry{index: index, term: term} ->
      not match_term?(index, term, log_store_impl)
    end)
    |> case do
      %Pb.Entry{index: index} -> index
      nil -> 0
    end
  end

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

  def campaign(%ReplicaState{term: term, self: %Models.Replica{id: id}} = state) do
    state
    |> reset(term + 1, false)
    |> set_leader_id(id)
    |> tick(false)
  end

  defp set_leader_id(%ReplicaState{} = state, id) do
    %ReplicaState{state | leader_id: id}
  end

  def became_leader(%ReplicaState{term: term} = state), do: became_leader(state, term)

  def became_leader(%ReplicaState{self: %Models.Replica{id: id}} = state, term) do
    state
    |> reset(term, true)
    |> set_leader_id(id)
  end

  def became_candidate(%ReplicaState{term: term, self: %Models.Replica{}} = state) do
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

  @spec vote_for(state :: ReplicaState.t(), vote_for_id :: Typespecs.id_t()) :: ReplicaState.t()
  def vote_for(state, vote_for_id) do
    %ReplicaState{state | voted_for: vote_for_id}
  end

  defp leader_message?(%Pb.Message{type: type}) do
    type in [:append_entries, :heartbeat]
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
end
