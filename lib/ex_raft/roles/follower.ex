defmodule ExRaft.Roles.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Roles.Common
  alias ExRaft.Typespecs

  require Logger

  def follower(:enter, _old_state, _state) do
    :keep_state_and_data
  end

  def follower(
        {:timeout, :tick},
        _,
        %ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state
      )
      when election_tick + 1 >= election_timeout do
    {:next_state, :candidate, Common.became_candidate(state), Common.tick_action(state)}
  end

  def follower({:timeout, :tick}, _, %ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
      when apply_tick + 1 > apply_timeout do
    state = Common.apply_to_statemachine(state)
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  def follower({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def follower(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(false, msg, state)
  end

  # heartbeat
  def follower(
        :cast,
        {:pipein, %Pb.Message{from: from_id, type: :heartbeat}},
        %ReplicaState{self: id, term: term} = state
      ) do
    Common.send_msg(state, %Pb.Message{
      type: :heartbeat_resp,
      to: from_id,
      from: id,
      term: term
    })

    {:keep_state, Common.became_follower(state, term, from_id)}
  end

  def follower(:cast, {:pipein, %Pb.Message{type: :request_vote} = msg}, %ReplicaState{} = state) do
    handle_request_vote(msg, state)
  end

  def follower(:cast, {:pipein, %Pb.Message{type: :append_entries} = msg}, %Models.ReplicaState{} = state) do
    handle_append_entries(msg, state)
  end

  def follower(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- propose ----------------
  def follower(:cast, {:propose, entries}, %ReplicaState{self: id, term: term, leader_id: leader_id} = state) do
    msg = %Pb.Message{
      type: :propose,
      term: term,
      from: id,
      to: leader_id,
      entries: entries
    }

    Common.send_msg(state, msg)

    :keep_state_and_data
  end

  # ------------------ internal event handler ------------------

  # ------------------ call event handler ------------------
  def follower({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :follower, state: state}})
    :keep_state_and_data
  end

  # ------------------ fallback -----------------

  def follower(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ------------ handle append entries ------------

  defp handle_append_entries(
         %Pb.Message{from: from_id, type: :append_entries} = msg,
         %Models.ReplicaState{term: current_term} = state
       ) do
    state =
      state
      |> do_append_entries(msg)
      |> Common.became_follower(current_term, from_id)

    {:keep_state, state}
  end

  # ------- handle_request_vote -------

  defp handle_request_vote(
         %Pb.Message{from: from_id} = msg,
         %ReplicaState{self: id, term: current_term, log_store_impl: log_store_impl} = state
       ) do
    {:ok, last_log} = LogStore.get_last_log_entry(log_store_impl)

    if Common.can_vote?(msg, state) and Common.log_updated?(msg, last_log) do
      Common.send_msg(state, %Pb.Message{
        type: :request_vote_resp,
        to: from_id,
        from: id,
        term: current_term,
        reject: false
      })

      {:keep_state, %Models.ReplicaState{state | voted_for: from_id, election_tick: 0}}
    else
      Common.send_msg(state, %Pb.Message{
        type: :request_vote_resp,
        to: from_id,
        from: id,
        term: current_term,
        reject: true
      })

      :keep_state_and_data
    end
  end

  @spec do_append_entries(state :: ReplicaState.t(), req :: Typespecs.message_t()) :: state :: ReplicaState.t()
  def do_append_entries(%ReplicaState{commit_index: commit_index, term: term} = state, %Pb.Message{
        log_index: log_index,
        from: from_id
      })
      when log_index < commit_index do
    Common.send_msg(state, %Pb.Message{
      to: from_id,
      type: :append_entries_response,
      term: term,
      log_index: commit_index,
      reject: true
    })

    state
  end

  def do_append_entries(
        %ReplicaState{term: term, log_store_impl: log_store_impl, last_log_index: last_index} = state,
        %Pb.Message{log_index: log_index, log_term: log_term, entries: entries, commit: leader_commit, from: from_id}
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

      Common.send_msg(state, %Pb.Message{
        to: from_id,
        type: :append_entries_response,
        term: term,
        log_index: log_index + cnt,
        reject: false
      })

      {peer, _} =
        state
        |> Common.local_peer()
        |> Models.Replica.try_update(log_index + cnt)

      Common.update_remote(
        %ReplicaState{state | last_log_index: log_index + cnt, commit_index: min(leader_commit, log_index + cnt)},
        peer
      )
    else
      Common.send_msg(state, %Pb.Message{
        to: from_id,
        type: :append_entries_response,
        term: term,
        log_index: log_index,
        reject: true
      })

      state
    end
  end

  def do_append_entries(%ReplicaState{} = state, _req), do: state

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
end
