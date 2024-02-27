defmodule ExRaft.Roles.Candidate do
  @moduledoc """
  Candidate Role Module

  Handle :gen_statm callbacks for candidate role
  """

  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Pipeline
  alias ExRaft.Roles.Common

  require Logger

  def candidate(:enter, _old_state, _state) do
    :keep_state_and_data
  end

  def candidate(
        {:timeout, :tick},
        _,
        %ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state
      )
      when election_tick + 1 >= election_timeout do
    {:keep_state, Common.tick(state, false), [{:next_event, :internal, :election} | Common.tick_action(state)]}
  end

  def candidate({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def candidate(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(false, msg, state)
  end

  # heartbeat
  def candidate(:cast, {:pipein, %Pb.Message{from: from_id, type: :heartbeat, term: term} = msg}, %ReplicaState{} = state) do
    {:next_state, Common.became_follower(state, term, from_id), Common.cast_action(msg)}
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :request_vote} = msg}, %ReplicaState{} = state) do
    handle_request_vote(msg, state)
  end

  def candidate(:cast, {:pipein, %Pb.Message{type: :request_vote_resp} = msg}, state) do
    handle_request_vote_reply(msg, state)
  end

  # While waiting for votes, a candidate may receive anWhile waiting for votes,
  # a candidate may receive an RPC from another server claiming a candidate may receive an
  # AppendEntries RPC from another server claiming to be the leader.
  # If the term number of this leader (contained in the RPC) is not smaller than the candidate's current term number,
  # then the candidate acknowledges the leader as legitimate and returns to the follower state. --- Raft paper5.2
  def candidate(
        :cast,
        {:pipein, %Pb.Message{from: from_id, type: :append_entries} = msg},
        %ReplicaState{term: current_term} = state
      ) do
    {:next_state, :follower, Common.became_follower(state, current_term, from_id), Common.cast_action(msg)}
  end

  def candidate(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore", %{msg: msg})
    :keep_state_and_data
  end

  # ------------------ internal event handler ------------------
  def candidate(:internal, :election, state), do: run_election(state)

  def candidate(event, data, state) do
    ExRaft.Debug.stacktrace(%{
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
    {:next_state, :leader, state}
  end

  defp run_election(state) do
    %ReplicaState{term: term, remotes: remotes, self: %Models.Replica{id: id}} =
      state = Common.campaign(state)

    ms =
      Enum.map(
        remotes,
        fn {to_id, _} ->
          %Pb.Message{
            type: :request_vote,
            to: to_id,
            from: id,
            term: term
          }
        end
      )

    Common.send_msgs(state, ms)

    {:keep_state, state}
  end

  # ------- handle_request_vote -------

  defp handle_request_vote(
         %Pb.Message{from: from_id} = req,
         %Models.ReplicaState{
           self: %Models.Replica{id: id},
           term: current_term,
           pipeline_impl: pipeline_impl,
           log_store_impl: log_store_impl
         } = state
       ) do
    {:ok, last_log} = LogStore.get_last_log_entry(log_store_impl)

    if Common.can_vote?(req, state) and Common.log_updated?(req, last_log) do
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

  # ------- handle_append_entries -------

  defp handle_append_entries(%Pb.Message{from: from_id} = msg, %ReplicaState{term: current_term} = state) do
  end

  # ---------------- handle_request_vote_reply ----------------

  defp handle_request_vote_reply(
         %Pb.Message{from: from_id, term: term, reject: rejected?},
         %ReplicaState{votes: votes, members_count: members_count} = state
       ) do
    votes = Map.put_new(votes, from_id, rejected?)

    accepted =
      Enum.count(votes, fn {_, rejected?} -> not rejected? end)

    if accepted * 2 > members_count do
      {:next_state, :leader, Common.became_leader(state, term), [{:next_event, :internal, :commit}]}
    else
      {:keep_state, %ReplicaState{state | votes: votes}}
    end
  end
end
