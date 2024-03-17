defmodule ExRaft.MessageHandlers.Prevote do
  @moduledoc false

  alias ExRaft.Core.Common
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  def handle(%Pb.Message{type: :heartbeat} = msg, state) do
    %Pb.Message{from: from_id, term: term} = msg
    {:next_state, Common.became_follower(state, term, from_id), Common.cast_action(msg)}
  end

  def handle(%Pb.Message{type: :propose}, _state) do
    Logger.warning("drop propose, no leader")
    :keep_state_and_data
  end

  def handle(%Pb.Message{type: :config_change}, _state) do
    Logger.warning("drop config_change, no leader")
    :keep_state_and_data
  end

  def handle(%Pb.Message{type: :append_entries} = msg, state) do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{term: current_term} = state
    {:next_state, :follower, Common.became_follower(state, current_term, from_id), Common.cast_action(msg)}
  end

  def handle(%Pb.Message{type: :request_vote} = msg, state) do
    Common.handle_request_vote(msg, state)
  end

  def handle(%Pb.Message{type: :request_pre_vote} = msg, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def handle(msg, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end
end
