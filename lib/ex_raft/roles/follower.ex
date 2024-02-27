defmodule ExRaft.Roles.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Pipeline
  alias ExRaft.Roles.Common

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
    {:next_state, :candidate, Common.became_candidate(state)}
  end

  def follower({:timeout, :tick}, _, %ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
      when apply_tick + 1 > apply_timeout do
    {:keep_state, Common.tick(state, false), [{:next_event, :internal, :apply} | Common.tick_action(state)]}
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
        %ReplicaState{self: %Models.Replica{id: id}, term: term} = state
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
    Logger.warning("Unknown message, ignore", %{msg: msg})
    :keep_state_and_data
  end

  # ------------------ internal event handler ------------------

  def follower(:internal, :apply, state), do: Common.apply_to_statemachine(state)

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
         %Models.ReplicaState{self: %Models.Replica{id: id}, term: current_term, last_log_index: last_index} = state
       ) do
    {cnt, commit_index, rejected?} = Common.do_append_entries(msg, state)

    commit_index <= last_index + cnt ||
      raise ExRaft.Exception,
        message: "invalid commit index",
        details: %{commit_index: commit_index, last_index: last_index + cnt}

    Common.send_msg(state, %Pb.Message{
      type: :append_entries_resp,
      to: from_id,
      from: id,
      term: current_term,
      reject: rejected?,
      log_index: commit_index
    })

    state = %Models.ReplicaState{
      state
      | last_log_index: last_index + cnt,
        commit_index: commit_index
    }

    {:keep_state, Common.became_follower(state, current_term, from_id)}
  end

  # ------- handle_request_vote -------

  defp handle_request_vote(
         %Pb.Message{from: from_id} = msg,
         %ReplicaState{
           self: %Models.Replica{id: id},
           term: current_term,
           pipeline_impl: pipeline_impl,
           log_store_impl: log_store_impl
         } = state
       ) do
    {:ok, last_log} = LogStore.get_last_log_entry(log_store_impl)

    if Common.can_vote?(msg, state) and Common.log_updated?(msg, last_log) do
      Pipeline.pipeout(pipeline_impl, [
        %Pb.Message{
          type: :request_vote_resp,
          to: from_id,
          from: id,
          term: current_term,
          reject: false
        }
      ])

      {:keep_state, %Models.ReplicaState{state | voted_for: from_id, election_tick: 0}}
    else
      Pipeline.pipeout(pipeline_impl, [
        %Pb.Message{
          type: :request_vote_resp,
          to: from_id,
          from: id,
          term: current_term,
          reject: true
        }
      ])

      :keep_state_and_data
    end
  end
end
