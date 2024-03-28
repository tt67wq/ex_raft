defmodule ExRaft.MessageHandlers.Candidate do
  @moduledoc false

  alias ExRaft.Core.Common
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  # heartbeat
  def handle(%Pb.Message{type: :heartbeat} = msg, state) do
    %Pb.Message{from: from_id, term: term} = msg
    {:next_state, Common.became_follower(state, term, from_id), Common.cast_pipein(msg)}
  end

  def handle(%Pb.Message{type: :request_vote} = msg, %ReplicaState{} = state) do
    Common.handle_request_vote(msg, state)
  end

  def handle(%Pb.Message{type: :request_vote_resp} = msg, state) do
    %Pb.Message{from: from_id, reject: rejected?} = msg
    %ReplicaState{votes: votes} = state
    votes = Map.put_new(votes, from_id, rejected?)
    state = %ReplicaState{state | votes: votes}

    if Common.vote_quorum_pass?(state) do
      {:next_state, :leader, Common.became_leader(state), [{:next_event, :internal, :broadcast_replica}]}
    else
      {:keep_state, state}
    end
  end

  def handle(%Pb.Message{type: :propose}, _state) do
    Logger.warning("drop propose, no leader")
    :keep_state_and_data
  end

  def handle(%Pb.Message{type: :config_change}, _state) do
    Logger.warning("drop config_change, no leader")
    :keep_state_and_data
  end

  # While waiting for votes, a candidate may receive anWhile waiting for votes,
  # a candidate may receive an RPC from another server claiming a candidate may receive an
  # AppendEntries RPC from another server claiming to be the leader.
  # If the term number of this leader (contained in the RPC) is not smaller than the candidate's current term number,
  # then the candidate acknowledges the leader as legitimate and returns to the follower state. --- Raft paper5.2
  def handle(%Pb.Message{type: :append_entries} = msg, state) do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{term: current_term} = state
    {:next_state, :follower, Common.became_follower(state, current_term, from_id), Common.cast_pipein(msg)}
  end

  def handle(%Pb.Message{type: :request_pre_vote} = msg, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def handle(msg, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end
end
