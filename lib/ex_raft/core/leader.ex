defmodule ExRaft.Core.Leader do
  @moduledoc """
  Leader Role Module

  Handle :gen_statm callbacks for leader role
  """
  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.MessageHandlers
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  def leader(:enter, _old_state, %ReplicaState{self: id}) do
    Logger.info("Replica #{id} become leader")
    :keep_state_and_data
  end

  def leader(
        {:timeout, :tick},
        _,
        %ReplicaState{heartbeat_tick: heartbeat_tick, heartbeat_timeout: heartbeat_timeout, members_count: members_count} =
          state
      )
      when members_count > 1 and heartbeat_tick + 1 > heartbeat_timeout do
    %ReplicaState{
      self: id,
      term: term,
      remotes: remotes,
      commit_index: commit_index
    } = state

    ms =
      remotes
      |> Enum.reject(fn {to_id, _} -> to_id == id end)
      |> Enum.map(fn {to_id, %Models.Replica{match: match}} ->
        %Pb.Message{
          type: :heartbeat,
          to: to_id,
          from: id,
          term: term,
          commit: min(match, commit_index)
        }
      end)

    Common.send_msgs(state, ms)

    state =
      state
      |> Common.reset_hearbeat()
      |> Common.tick(true)

    {:keep_state, state, Common.tick_action(state)}
  end

  def leader(
        {:timeout, :tick},
        _,
        %ReplicaState{election_timeout: election_timeout, election_tick: election_tick, members_count: members_count} =
          state
      )
      when members_count > 1 and election_tick > election_timeout do
    %ReplicaState{
      self: local,
      term: term,
      remotes: remotes
    } = state

    c =
      Enum.count(remotes, fn
        {^local, _peer} -> true
        {_, %Models.Replica{active?: active?}} -> active?
      end)

    if c > Common.quorum(state) do
      state =
        state
        |> Common.reset(term)
        |> Common.set_all_remotes_inactive()
        |> Common.tick(true)

      {:keep_state, state, Common.tick_action(state)}
    else
      Logger.warning("Leader #{local} become follower due to quorum fail")

      state =
        state
        |> Common.became_follower(term, 0)
        |> Common.tick(false)

      {:next_state, :follower, state, Common.tick_action(state)}
    end
  end

  def leader({:timeout, :tick}, _, %ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
      when apply_tick + 1 > apply_timeout do
    state =
      state
      |> Common.apply_to_statemachine()
      |> Common.tick(true)

    {:keep_state, state, Common.tick_action(state)}
  end

  def leader({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, true), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------

  # on term mismatch
  def leader(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(:leader, msg, state)
  end

  # msg from non-exists peer
  def leader(:cast, {:pipein, %Pb.Message{from: from} = msg}, state) when from != 0 do
    %ReplicaState{remotes: remotes} = state

    remotes
    |> Map.has_key?(from)
    |> if do
      MessageHandlers.Leader.handle(msg, state)
    else
      :keep_state_and_data
    end
  end

  def leader(:cast, {:pipein, msg}, state), do: MessageHandlers.Leader.handle(msg, state)

  # ---------------- propose ----------------

  def leader(:cast, {:propose, entries}, state) do
    %ReplicaState{self: id, term: term} = state

    msg = %Pb.Message{
      type: :propose,
      term: term,
      from: id,
      entries: entries
    }

    {:keep_state_and_data, Common.cast_action(msg)}
  end

  # ------------------ internal event handler ------------------

  def leader(:internal, :commit, state) do
    %ReplicaState{
      remotes: remotes,
      commit_index: commit_index,
      log_store_impl: log_store_impl,
      term: current_term
    } =
      state

    # get middle index
    {_, %Models.Replica{match: match}} =
      remotes
      |> Enum.sort()
      |> Enum.at(Common.quorum(state) - 1)

    {:ok, term} = LogStore.get_log_term(log_store_impl, match)

    if match > commit_index and term == current_term do
      {:keep_state, Common.commit_to(state, match)}
    else
      :keep_state_and_data
    end
  end

  def leader(:internal, :broadcast_replica, state) do
    %ReplicaState{self: id, remotes: remotes} = state

    state =
      remotes
      |> Enum.reject(fn {to_id, _} -> to_id == id end)
      |> Enum.reduce(state, fn {_, peer}, acc ->
        Common.send_replicate_msg(acc, peer)
      end)
      |> Common.reset_hearbeat()

    {:keep_state, state}
  end

  # ------------------ call event handler ------------------
  def leader({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :leader, state: state}})
    :keep_state_and_data
  end

  # --------------------- fallback -----------------

  def leader(event, data, state) do
    Logger.debug(%{
      role: :leader,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end
end
