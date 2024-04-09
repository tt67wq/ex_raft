defmodule ExRaft.Core.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  import ExRaft.Guards

  alias ExRaft.Core.Common
  alias ExRaft.MessageHandlers
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  def follower(:enter, _old_state, %ReplicaState{self: id}) do
    Logger.info("Replica #{id} become follower")
    :keep_state_and_data
  end

  def follower({:timeout, :tick}, _, state) do
    if Common.self_removed?(state) do
      Logger.warning("This replica has been removed")
      :keep_state_and_data
    else
      tick(state)
    end
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def follower(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(:follower, msg, state)
  end

  # msg from non-exists peer
  def follower(:cast, {:pipein, %Pb.Message{from: from} = msg}, state) when not_empty(from) do
    %ReplicaState{remotes: remotes} = state

    remotes
    |> Map.has_key?(from)
    |> if do
      MessageHandlers.Follower.handle(msg, state)
    else
      Logger.warning("Receive msg from non-exists peer: #{inspect(msg)}")
      :keep_state_and_data
    end
  end

  def follower(:cast, {:pipein, msg}, state) do
    MessageHandlers.Follower.handle(msg, state)
  end

  # ---------------- propose ----------------
  def follower(:cast, {:propose, entries}, state) do
    %ReplicaState{self: id, term: term, leader_id: leader_id} = state

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

  # ---------------- config change ----------------
  def follower(:cast, {:config_change, entries}, state) do
    %ReplicaState{self: id, term: term, leader_id: leader_id} = state

    msg = %Pb.Message{
      type: :config_change,
      term: term,
      from: id,
      to: leader_id,
      entries: entries
    }

    Common.send_msg(state, msg)

    :keep_state_and_data
  end

  # ------------------ call event handler ------------------
  def follower({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :follower, state: state}})
    :keep_state_and_data
  end

  # ------------------ fallback -----------------

  def follower(event, data, state) do
    Logger.debug(%{
      role: :follower,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ----------------- tick ---------------
  defp tick(%ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state)
       when election_tick + 1 >= election_timeout do
    {:next_state, :prevote, Common.became_prevote(state), Common.tick_action(state)}
  end

  defp tick(%ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
       when apply_tick + 1 > apply_timeout do
    state = Common.apply_to_statemachine(state)
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  defp tick(state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end
end
