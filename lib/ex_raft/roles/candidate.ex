defmodule ExRaft.Roles.Candidate do
  @moduledoc """
  Candidate Role Module

  Handle :gen_statm callbacks for candidate role
  """

  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Roles.Common

  require Logger

  def candidate(:enter, _old_state, %ReplicaState{self: id}) do
    Logger.info("Replica #{id} become candidate")
    :keep_state_and_data
  end

  def candidate(
        {:timeout, :tick},
        _,
        %ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state
      )
      when election_tick + 1 >= election_timeout do
    Logger.warning("election timeout, start campaign")
    run_election(state)
  end

  def candidate({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def candidate(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(:candidate, msg, state)
  end

  # heartbeat
  def candidate(:cast, {:pipein, %Pb.Message{type: :heartbeat} = msg}, state) do
    %Pb.Message{from: from_id, term: term} = msg
    {:next_state, Common.became_follower(state, term, from_id), Common.cast_action(msg)}
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :request_vote} = msg}, %ReplicaState{} = state) do
    Common.handle_request_vote(msg, state)
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :request_vote_resp} = msg}, state) do
    %Pb.Message{from: from_id, term: term, reject: rejected?} = msg
    %ReplicaState{votes: votes} = state
    votes = Map.put_new(votes, from_id, rejected?)
    state = %ReplicaState{state | votes: votes}

    if Common.vote_quorum_pass?(state) do
      {:next_state, :leader, Common.became_leader(state, term)}
    else
      {:keep_state, state}
    end
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :propose}}, _state) do
    Logger.warning("drop propose, no leader")
    :keep_state_and_data
  end

  # While waiting for votes, a candidate may receive anWhile waiting for votes,
  # a candidate may receive an RPC from another server claiming a candidate may receive an
  # AppendEntries RPC from another server claiming to be the leader.
  # If the term number of this leader (contained in the RPC) is not smaller than the candidate's current term number,
  # then the candidate acknowledges the leader as legitimate and returns to the follower state. --- Raft paper5.2
  def candidate(:cast, {:pipein, %Pb.Message{type: :append_entries} = msg}, state) do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{term: current_term} = state
    {:next_state, :follower, Common.became_follower(state, current_term, from_id), Common.cast_action(msg)}
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :request_pre_vote} = msg}, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def candidate(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- propose ----------------

  def candidate(:cast, {:propose, _entries}, _state) do
    Logger.warning("drop propose, no leader")
    :keep_state_and_data
  end

  # ------------------ internal event handler ------------------

  # ------------------ call event handler ------------------
  def candidate({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :candidate, state: state}})
    :keep_state_and_data
  end

  # ----------------- fallback -----------------

  def candidate(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      role: :candidate,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ------- run_election -------

  defp run_election(%ReplicaState{members_count: 1} = state) do
    # no other peers, become leader
    state = state |> Common.campaign() |> Common.became_leader()
    {:next_state, :leader, Common.tick_action(state)}
  end

  defp run_election(state) do
    %ReplicaState{term: term, remotes: remotes, self: id} =
      state = Common.campaign(state)

    ms =
      remotes
      |> Enum.reject(fn {x, _} -> x == id end)
      |> Enum.map(fn {to_id, _} ->
        %Pb.Message{
          type: :request_vote,
          to: to_id,
          from: id,
          term: term
        }
      end)

    Common.send_msgs(state, ms)

    {:keep_state, state, Common.tick_action(state)}
  end
end
