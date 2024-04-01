defmodule ExRaft.Core.Prevote do
  @moduledoc """
  Prevote Role Module
  """
  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.MessageHandlers
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb

  require Logger

  def prevote(:enter, _old_state, %ReplicaState{self: id}) do
    Logger.info("Replica #{id} become prevote")
    :keep_state_and_data
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

  # msg from non-exists peer
  def prevote(:cast, {:pipein, %Pb.Message{from: from} = msg}, state) when from != 0 do
    %ReplicaState{remotes: remotes} = state

    remotes
    |> Map.has_key?(from)
    |> if do
      MessageHandlers.Prevote.handle(msg, state)
    else
      :keep_state_and_data
    end
  end

  def prevote(:cast, {:pipein, msg}, state) do
    MessageHandlers.Prevote.handle(msg, state)
  end

  # ------------------ call event handler ------------------
  def prevote({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :prevote, state: state}})
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
    %ReplicaState{term: term, remotes: remotes, self: id, last_index: last_index, log_store_impl: log_store_impl} =
      state = Common.prevote_campaign(state)

    log_term =
      log_store_impl
      |> LogStore.get_last_log_entry()
      |> case do
        {:ok, %Pb.Entry{term: log_term}} -> log_term
        _ -> 0
      end

    ms =
      remotes
      |> Enum.reject(fn {x, _} -> x == id end)
      |> Enum.map(fn {to_id, _} ->
        %Pb.Message{
          type: :request_pre_vote,
          to: to_id,
          from: id,
          term: term + 1,
          log_index: last_index,
          log_term: log_term
        }
      end)

    Common.send_msgs(state, ms)

    {:keep_state, state, Common.tick_action(state)}
  end
end
