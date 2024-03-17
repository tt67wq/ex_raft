defmodule ExRaft.Core.Prevote do
  @moduledoc """
  Prevote Role Module
  """
  alias ExRaft.Core.Common
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  def prevote(:enter, _old_state, %ReplicaState{self: id, remotes: remotes} = state) do
    Logger.info("Replica #{id} become prevote")

    if Map.has_key?(remotes, id) do
      :keep_state_and_data
    else
      # myself has been kickout, be a free node
      {:next_state, :free, state}
    end
  end

  def prevote(
        {:timeout, :tick},
        _,
        %ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state
      )
      when election_tick + 1 >= election_timeout do
    Logger.warning("election timeout, start prevote")
    run_prevote(state)
  end

  def prevote({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def prevote(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(:prevote, msg, state)
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :heartbeat} = msg}, state) do
    %Pb.Message{from: from_id, term: term} = msg
    {:next_state, Common.became_follower(state, term, from_id), Common.cast_action(msg)}
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :propose}}, _state) do
    Logger.warning("drop propose, no leader")
    :keep_state_and_data
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :config_change}}, _state) do
    Logger.warning("drop config_change, no leader")
    :keep_state_and_data
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :append_entries} = msg}, state) do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{term: current_term} = state
    {:next_state, :follower, Common.became_follower(state, current_term, from_id), Common.cast_action(msg)}
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :request_vote} = msg}, state) do
    Common.handle_request_vote(msg, state)
  end

  def prevote(:cast, {:pipein, %Pb.Message{type: :request_pre_vote} = msg}, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def prevote(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ----------------- fallback -----------------

  def prevote(event, data, state) do
    Logger.debug(%{
      role: :prevote,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  defp run_prevote(%ReplicaState{members_count: 1} = state) do
    state = Common.became_candidate(state)
    {:next_state, :candidate, state, Common.tick_action(state)}
  end

  defp run_prevote(state) do
    %ReplicaState{term: term, remotes: remotes, self: id} =
      state = Common.prevote_campaign(state)

    ms =
      remotes
      |> Enum.reject(fn {x, _} -> x == id end)
      |> Enum.map(fn {to_id, _} ->
        %Pb.Message{
          type: :request_pre_vote,
          to: to_id,
          from: id,
          term: term + 1
        }
      end)

    Common.send_msgs(state, ms)

    {:keep_state, state, Common.tick_action(state)}
  end
end
