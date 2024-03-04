defmodule ExRaft.Roles.Leader do
  @moduledoc """
  Leader Role Module

  Handle :gen_statm callbacks for leader role
  """
  alias ExRaft.Config
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Roles.Common
  alias ExRaft.Typespecs

  require Logger

  def leader(:enter, _old_state, _state) do
    :keep_state_and_data
  end

  def leader(
        {:timeout, :tick},
        _,
        %ReplicaState{
          self: %Models.Replica{id: id},
          term: term,
          heartbeat_tick: heartbeat_tick,
          heartbeat_timeout: heartbeat_timeout,
          remotes: remotes,
          log_store_impl: log_store_impl,
          last_log_index: last_log_index,
          commit_index: commit_index
        } = state
      )
      when heartbeat_tick + 1 > heartbeat_timeout do
    ms =
      Enum.map(remotes, fn {to_id, %Models.Replica{match: prev_index}} ->
        {:ok, prev_term} = LogStore.get_log_term(log_store_impl, prev_index)
        {:ok, entries} = LogStore.get_range(log_store_impl, prev_index, last_log_index)

        %Pb.Message{
          type: :heartbeat,
          to: to_id,
          from: id,
          term: term,
          log_index: prev_index,
          log_term: prev_term,
          entries: entries,
          commit: commit_index
        }
      end)

    Common.send_msgs(state, ms)

    state =
      state
      |> Common.reset(term, true)
      |> Common.tick(true)

    {:keep_state, state, Common.tick_action(state)}
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
    Common.handle_term_mismatch(false, msg, state)
  end

  def leader(
        :cast,
        {:pipein, %Pb.Message{from: from_id, type: :request_vote}},
        %ReplicaState{self: %Models.Replica{id: id}, term: current_term} = state
      ) do
    Common.send_msg(state, %Pb.Message{
      type: :request_vote_resp,
      to: from_id,
      from: id,
      term: current_term,
      reject: true
    })

    :keep_state_and_data
  end

  def leader(
        :cast,
        {:pipein, %Pb.Message{from: from_id, type: :heartbeat_resp}},
        %ReplicaState{remotes: remotes, last_log_index: last_index} = state
      ) do
    %Models.Replica{match: match} = peer = Map.fetch!(remotes, from_id)

    if last_index > match do
      # send log replica msg
      peer
      |> make_replicate_msg(state)
      |> case do
        nil ->
          :keep_state_and_data

        %Pb.Message{entries: entries} = msg ->
          Common.send_msg(state, msg)
          %Pb.Entry{index: index} = List.last(entries)
          peer = Map.put(peer, :next, index + 1)
          remotes = Map.put(remotes, from_id, peer)

          {:keep_state, %ReplicaState{state | remotes: remotes}}
      end
    else
      :keep_state_and_data
    end
  end

  def leader(
        :cast,
        {:pipein, %Pb.Message{type: :append_entries_resp, reject: false, from: from_id, log_index: log_index}},
        %ReplicaState{remotes: remotes} = state
      ) do
    %{^from_id => %Models.Replica{next: next, match: match} = peer} = remotes
    next = max(next, log_index + 1)
    match = (match < log_index && log_index) || match
    peer = %Models.Replica{peer | next: next, match: match}
    {:keep_state, %ReplicaState{state | remotes: Map.put(remotes, from_id, peer)}}
  end

  def leader(
        :cast,
        {:pipein, %Pb.Message{type: :propose, entries: entries}},
        %ReplicaState{term: term, last_log_index: last_log_index, log_store_impl: log_store_impl} = state
      ) do
    entries =
      entries
      |> Enum.with_index(last_log_index + 1)
      |> Enum.map(fn {entry, index} ->
        %Pb.Entry{entry | term: term, index: index}
      end)

    {:ok, cnt} = LogStore.append_log_entries(log_store_impl, entries)
    ExRaft.Debug.debug("append_log_entries: #{cnt}")

    {:keep_state, %ReplicaState{state | last_log_index: last_log_index + cnt}}
  end

  # other pipein, ignore
  def leader(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- propose ----------------

  def leader(:cast, {:propose, entries}, %ReplicaState{self: %Models.Replica{id: id}, term: term}) do
    msg = %Pb.Message{
      type: :propose,
      term: term,
      from: id,
      entries: entries
    }

    {:keep_state_and_data, Common.cast_action(msg)}
  end

  # ------------------ internal event handler ------------------

  # ------------------ call event handler ------------------
  def leader({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :leader, state: state}})
    :keep_state_and_data
  end

  # --------------------- fallback -----------------

  def leader(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ----------------- make replicate msgs ---------------
  @spec make_replicate_msg(Models.Replica.t(), ReplicaState.t()) :: Typespecs.message_t() | nil
  defp make_replicate_msg(%Models.Replica{id: to_id, next: next}, %ReplicaState{
         self: %Models.Replica{id: id},
         log_store_impl: log_store_impl,
         term: term
       }) do
    {:ok, log_term} = LogStore.get_log_term(log_store_impl, next - 1)
    {:ok, entries} = LogStore.get_range(log_store_impl, next, Config.max_msg_batch_size())

    case entries do
      [] ->
        nil

      _ ->
        %Pb.Message{
          to: to_id,
          from: id,
          type: :append_entries,
          term: term,
          log_index: next - 1,
          log_term: log_term,
          entries: entries
        }
    end
  end
end
