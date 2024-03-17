defmodule ExRaft.Core.Candidate do
  @moduledoc """
  Candidate Role Module

  Handle :gen_statm callbacks for candidate role
  """

  import ExRaft.Guards

  alias ExRaft.Core.Common
  alias ExRaft.MessageHandlers
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

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

  # msg from non-exists peer
  def candidate(:cast, {:pipein, %Pb.Message{from: from} = msg}, state) when not_empty(from) do
    %ReplicaState{remotes: remotes} = state

    remotes
    |> Map.has_key?(from)
    |> if do
      MessageHandlers.Candidate.handle(msg, state)
    else
      Logger.warning("drop msg from non-exists peer")
      :keep_state_and_data
    end
  end

  def candidate(:cast, {:pipein, msg}, state) do
    MessageHandlers.Candidate.handle(msg, state)
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
    Logger.debug(%{
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
    Logger.debug("no other peers, become leader")
    state = state |> Common.campaign() |> Common.became_leader()
    {:next_state, :leader, state, [{:next_event, :internal, :broadcast_replica} | Common.tick_action(state)]}
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
